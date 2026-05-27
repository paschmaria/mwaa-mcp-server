"""Resource-link pagination for large MWAA responses.

Returns small summaries with a resource URI clients can follow up on, instead
of dumping multi-KB blobs that get truncated. URIs are stateless — every
parameter needed to re-fetch the slice is encoded in the URI itself.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, quote, urlencode, urlparse

# Strips ANSI color/style escapes that dbt and other CLIs emit (`\x1b[31m` etc.).
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

# CloudWatch task handler echoes wrapper lines even when the underlying
# log stream returns no events. These aren't log content — Airflow's
# CloudWatchRemoteLogIO describes what it tried to fetch. Strip them so
# a missing/rotated log surfaces as empty rather than as
# "Reading remote log from Cloudwatch ...\n'nextForwardToken'".
_CW_ENVELOPE_RE = re.compile(
    r"^(?:Reading remote log from Cloudwatch[^\n]*"
    r"|'nextForwardToken'\s*"
    r"|\*\*\* Falling back to local log[^\n]*)$",
    re.MULTILINE,
)

# Default page size — small enough to avoid hitting host token limits, large
# enough that one page is usually sufficient for "what failed recently?" type
# questions.
DEFAULT_PAGE_SIZE = 25

# Hard cap on how many lines of log text the tool inlines before pointing at a
# resource for the rest. Keeps the tool response under typical token budgets.
LOG_INLINE_HEAD_LINES = 60
LOG_INLINE_TAIL_LINES = 60


def build_resource_uri(scheme_path: str, params: Dict[str, Any]) -> str:
    """Build a `mwaa://...` URI with query params encoding the slice request."""
    cleaned = {k: v for k, v in params.items() if v is not None and v != ""}
    query = urlencode(cleaned, doseq=True, quote_via=quote)
    return f"mwaa://{scheme_path}?{query}" if query else f"mwaa://{scheme_path}"


def parse_resource_uri(uri: str) -> tuple[List[str], Dict[str, str]]:
    """Parse a `mwaa://...` URI into (path_parts, query_dict).

    Returns the path split on '/' and a flat dict of the query parameters.
    """
    parsed = urlparse(uri)
    if parsed.scheme != "mwaa":
        raise ValueError(f"Expected mwaa:// URI, got {parsed.scheme}://")
    # urlparse puts everything after `mwaa://` into netloc; join with path.
    full_path = parsed.netloc + parsed.path
    path_parts = [p for p in full_path.split("/") if p]
    query: Dict[str, str] = {}
    for k, v in parse_qs(parsed.query).items():
        # `parse_qs` returns list-valued dict; we want last value (form behavior).
        query[k] = v[-1] if v else ""
    return path_parts, query


def slice_list(
    items: List[Any],
    page: int,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> tuple[List[Any], bool]:
    """Return (page_items, has_more)."""
    page = max(1, page)
    start = (page - 1) * page_size
    end = start + page_size
    return items[start:end], end < len(items)


def with_resource_link(
    *,
    summary: Dict[str, Any],
    items: List[Any],
    page: int,
    page_size: int,
    resource_path: str,
    resource_params: Dict[str, Any],
    item_key: str = "items",
) -> Dict[str, Any]:
    """Standard envelope: summary + first-page items + next-page resource URI.

    The shape is intentionally generic so list endpoints all look the same:

        {
          "summary": {...},
          "items": [...],
          "pagination": {
            "page": 1, "page_size": 25, "total_count": 109, "has_more": true,
            "next_resource_uri": "mwaa://..."
          }
        }
    """
    page_items, has_more = slice_list(items, page, page_size)
    total = len(items)
    pagination: Dict[str, Any] = {
        "page": page,
        "page_size": page_size,
        "total_count": total,
        "has_more": has_more,
    }
    if has_more:
        pagination["next_resource_uri"] = build_resource_uri(
            resource_path, {**resource_params, "page": page + 1, "page_size": page_size}
        )
    return {"summary": summary, item_key: page_items, "pagination": pagination}


def truncate_log_text(
    log_text: str,
    head_lines: int = LOG_INLINE_HEAD_LINES,
    tail_lines: int = LOG_INLINE_TAIL_LINES,
) -> tuple[str, Dict[str, Any]]:
    """Return (inlined_text, metadata) for a log body.

    If the log is short enough, returns it verbatim. Otherwise returns
    head+tail joined by a marker, plus metadata with the original line count.
    """
    lines = log_text.splitlines()
    total = len(lines)
    if total <= head_lines + tail_lines:
        return log_text, {"total_lines": total, "truncated": False}
    head = lines[:head_lines]
    tail = lines[-tail_lines:]
    body = (
        "\n".join(head)
        + f"\n\n... [truncated {total - head_lines - tail_lines} lines —"
        f" fetch the resource URI for the full log] ...\n\n"
        + "\n".join(tail)
    )
    return body, {
        "total_lines": total,
        "truncated": True,
        "head_lines": head_lines,
        "tail_lines": tail_lines,
    }


def encode_log_resource_uri(
    environment_name: str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_try_number: Optional[int] = None,
) -> str:
    """Resource URI for the full text of one task instance's log."""
    # Path-encode each segment so dag_run_id (with colons/+/slashes) survives.
    parts = [
        "logs",
        quote(environment_name, safe=""),
        quote(dag_id, safe=""),
        quote(dag_run_id, safe=""),
        quote(task_id, safe=""),
    ]
    params: Dict[str, Any] = {}
    if task_try_number is not None:
        params["try_number"] = task_try_number
    return build_resource_uri("/".join(parts), params)


def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)


def _strip_cw_envelope(s: str) -> str:
    """Drop CloudWatch task handler wrapper lines from a log body."""
    cleaned = _CW_ENVELOPE_RE.sub("", s)
    # Collapse the blank lines we just punched out.
    return re.sub(r"\n{3,}", "\n\n", cleaned).strip("\n")


def _event_to_line(entry: Any) -> str:
    """Render one structured log entry from Airflow into a single readable line.

    Airflow returns each log "line" as a JSON object like:

        {"timestamp": "...", "event": "actual log message", "level": "info", ...}

    or sometimes nested with ``error_detail`` for tracebacks. Pull the
    user-visible message out so downstream regex heuristics see the actual
    text rather than ``str(dict)``.
    """
    if isinstance(entry, str):
        return _strip_ansi(entry)
    if not isinstance(entry, dict):
        return _strip_ansi(str(entry))

    event = entry.get("event")
    if isinstance(event, str):
        return _strip_ansi(event)
    if isinstance(event, list):
        # Some entries (tracebacks) carry event as a list of frames.
        return _strip_ansi("\n".join(str(e) for e in event))

    # Fall back to the message field; some logs use it instead of event.
    msg = entry.get("message")
    if isinstance(msg, str):
        return _strip_ansi(msg)

    # Last resort — stringify but drop the noisy framing.
    return _strip_ansi(str(entry))


def extract_log_text(raw_response: Dict[str, Any]) -> str:
    """Pull the log body out of MWAA's `invoke_rest_api` response shape.

    Airflow returns logs under `RestApiResponse.content` as either:
    - a list of structured entries (each a dict with ``event``/``timestamp``)
    - a list of strings
    - a single string

    This function normalizes all three into a plain text body (one entry per
    line) and strips ANSI escape codes so downstream regex heuristics work.
    """
    body = raw_response.get("RestApiResponse", raw_response)
    if isinstance(body, str):
        return _strip_cw_envelope(_strip_ansi(body))
    if not isinstance(body, dict):
        return _strip_cw_envelope(_strip_ansi(str(body)))

    content = body.get("content")
    if isinstance(content, list):
        return _strip_cw_envelope(
            "\n".join(_event_to_line(c) for c in content)
        )
    if isinstance(content, str):
        return _strip_cw_envelope(_strip_ansi(content))
    # Unknown shape — return a best-effort string so the caller still sees
    # something rather than nothing.
    return _strip_cw_envelope(_strip_ansi(str(body)))
