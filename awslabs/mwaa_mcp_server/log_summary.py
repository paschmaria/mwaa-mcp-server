"""Heuristics for extracting the relevant lines from a task log.

Airflow task logs are mostly noise (init/teardown, per-step status output)
with a few signal lines buried inside. This module turns a raw log body
into a short structured summary so the model sees the actual failure cause
rather than the whole 50KB blob.

Matchers in order of headline priority:

- dbt test/build failures (``N of M FAIL ...``, ``Failure in test ...``)
- Python exceptions (``ValueError: ...``)
- Python tracebacks (``Traceback (most recent call last):``)
- Airflow task-failed markers (``Task failed with exception``)
- Non-zero exit codes (``exit code: 1``, ``return code 137``)

dbt is included by default because Airflow-on-MWAA deployments use it
heavily (via Cosmos and DbtRunLocalOperator-style operators) and a dbt
FAIL is almost always the most diagnostic line in the log — more useful
than the Airflow wrapper ``Task failed`` marker that follows it. The
``Done. PASS=X WARN=Y ERROR=Z TOTAL=N`` summary line is parsed into a
small stats dict when present.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

# Each pattern's match line + its surrounding context window is reported.
# Order in `summarize_log` determines headline priority when multiple match.

PYTHON_TRACEBACK_RE = re.compile(r"^Traceback \(most recent call last\):", re.MULTILINE)
PYTHON_EXCEPTION_RE = re.compile(
    r"^(\w+(?:Error|Exception|Warning|Interrupt)):\s*(.+)$", re.MULTILINE
)
AIRFLOW_TASK_FAILED_RE = re.compile(
    r"^.*?Task (failed|exited) with .*$", re.MULTILINE
)
NON_ZERO_EXIT_RE = re.compile(
    r"^.*?(?:exit code|exited with code|return code)[:\s]+(\d+)\b.*$",
    re.MULTILINE | re.IGNORECASE,
)

# dbt per-test status lines:
#   "15:09:32  1 of 64 FAIL 12 some_test_name ........ [FAIL 12 in 1.2s]"
#   "15:09:32  1 of 64 ERROR some_model_name ........ [ERROR in 0.4s]"
# Anchored to "<N> of <M>" so we don't catch the word FAIL inside other lines.
DBT_FAIL_RE = re.compile(
    r"^.*?\d+ of \d+ (?:FAIL|ERROR)(?:\s+\d+)?\s+\S+.*$",
    re.MULTILINE,
)

# dbt per-failure detail blocks emitted after the test summary:
#   "Failure in test some_test (models/path/_models.yml)"
DBT_FAILURE_DETAIL_RE = re.compile(
    r"^.*?Failure in (?:test|model|generic test) \S+.*$",
    re.MULTILINE,
)

# dbt runtime/database/compilation errors (raised by adapters):
#   "Runtime Error in model my_model (models/path.sql)"
#   "Database Error in model my_model (models/path.sql)"
#   "Compilation Error in model my_model (models/path.sql)"
DBT_RUNTIME_ERROR_RE = re.compile(
    r"^.*?(?:Runtime Error|Database Error|Compilation Error)(?:\s+in|:).*$",
    re.MULTILINE,
)

# dbt final summary line:
#   "15:09:32  Done. PASS=62 WARN=2 ERROR=0 SKIP=0 NO-OP=0 TOTAL=64"
# NO-OP was added in dbt 1.8; treat it as optional so 1.7-and-earlier logs
# still parse.
DBT_DONE_RE = re.compile(
    r"Done\.\s+PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)\s+SKIP=(\d+)"
    r"(?:\s+NO-OP=(\d+))?\s+TOTAL=(\d+)"
)


@dataclass
class LogHit:
    """One matched line plus its surrounding context."""

    category: str
    line_no: int
    line: str
    context_before: List[str] = field(default_factory=list)
    context_after: List[str] = field(default_factory=list)


@dataclass
class LogSummary:
    """Structured failure summary returned by summarize_log."""

    headline: Optional[str]
    dbt_test_failures: List[LogHit] = field(default_factory=list)
    dbt_done_stats: Optional[Dict[str, int]] = None
    python_exceptions: List[LogHit] = field(default_factory=list)
    python_tracebacks: List[LogHit] = field(default_factory=list)
    airflow_task_failures: List[LogHit] = field(default_factory=list)
    non_zero_exits: List[LogHit] = field(default_factory=list)
    total_lines: int = 0

    def to_dict(self) -> Dict[str, object]:
        def hits(xs: List[LogHit]) -> List[Dict[str, object]]:
            return [
                {
                    "line_no": h.line_no,
                    "line": h.line,
                    "context_before": h.context_before,
                    "context_after": h.context_after,
                }
                for h in xs
            ]

        return {
            "headline": self.headline,
            "total_lines": self.total_lines,
            "dbt_test_failures": hits(self.dbt_test_failures),
            "dbt_done_stats": self.dbt_done_stats,
            "python_exceptions": hits(self.python_exceptions),
            "python_tracebacks": hits(self.python_tracebacks),
            "airflow_task_failures": hits(self.airflow_task_failures),
            "non_zero_exits": hits(self.non_zero_exits),
        }


def _make_hit(
    category: str, line_no: int, lines: List[str], context: int = 4
) -> LogHit:
    before_start = max(0, line_no - context)
    after_end = min(len(lines), line_no + context + 1)
    return LogHit(
        category=category,
        line_no=line_no + 1,  # human-friendly 1-based
        line=lines[line_no].rstrip(),
        context_before=[lines[i].rstrip() for i in range(before_start, line_no)],
        context_after=[lines[i].rstrip() for i in range(line_no + 1, after_end)],
    )


def _line_indices(pattern: re.Pattern, text: str) -> List[int]:
    """Return 0-based line indices in `text` where `pattern` matches."""
    indices = []
    pos = 0
    line_no = 0
    for m in pattern.finditer(text):
        line_no += text.count("\n", pos, m.start())
        pos = m.start()
        indices.append(line_no)
    return indices


def summarize_log(log_text: str, context_lines: int = 4) -> LogSummary:
    """Run heuristics over the log text and return a LogSummary.

    Matchers (in headline-priority order):
    - dbt test/build failures (``N of M FAIL ...``, ``Failure in test ...``,
      ``Runtime Error in model ...``)
    - Python exceptions (``ValueError: ...``, ``RuntimeError: ...``)
    - Python tracebacks (``Traceback (most recent call last):``)
    - Airflow task-failed markers (``Task failed with exception``)
    - Non-zero exit codes (``exit code: 1``, ``return code 137``)

    The dbt ``Done. PASS=X WARN=Y ERROR=Z TOTAL=N`` summary line is parsed
    out separately into ``dbt_done_stats`` (numeric counts) when present —
    useful for telling "build with 0 errors and warnings" apart from "build
    with errors" without re-scanning the log.
    """
    lines = log_text.splitlines() or [""]
    summary = LogSummary(headline=None, total_lines=len(lines))

    # dbt patterns first (highest headline priority in Airflow-on-MWAA
    # deployments where Cosmos / dbt is the dominant operator type).
    for line_no in _line_indices(DBT_FAIL_RE, log_text):
        summary.dbt_test_failures.append(
            _make_hit("dbt_test_failure", line_no, lines, context_lines)
        )
    for line_no in _line_indices(DBT_FAILURE_DETAIL_RE, log_text):
        summary.dbt_test_failures.append(
            _make_hit("dbt_failure_detail", line_no, lines, context_lines)
        )
    for line_no in _line_indices(DBT_RUNTIME_ERROR_RE, log_text):
        summary.dbt_test_failures.append(
            _make_hit("dbt_runtime_error", line_no, lines, max(context_lines, 8))
        )

    done_match = DBT_DONE_RE.search(log_text)
    if done_match:
        pass_n, warn_n, error_n, skip_n, noop_n, total_n = done_match.groups()
        summary.dbt_done_stats = {
            "pass": int(pass_n),
            "warn": int(warn_n),
            "error": int(error_n),
            "skip": int(skip_n),
            "no_op": int(noop_n) if noop_n is not None else 0,
            "total": int(total_n),
        }

    for line_no in _line_indices(PYTHON_EXCEPTION_RE, log_text):
        summary.python_exceptions.append(
            _make_hit("python_exception", line_no, lines, context_lines)
        )
    for line_no in _line_indices(PYTHON_TRACEBACK_RE, log_text):
        summary.python_tracebacks.append(
            _make_hit("python_traceback", line_no, lines, max(context_lines, 12))
        )
    for line_no in _line_indices(AIRFLOW_TASK_FAILED_RE, log_text):
        summary.airflow_task_failures.append(
            _make_hit("airflow_task_failed", line_no, lines, context_lines)
        )
    for line_no in _line_indices(NON_ZERO_EXIT_RE, log_text):
        summary.non_zero_exits.append(_make_hit("non_zero_exit", line_no, lines, context_lines))

    summary.headline = _pick_headline(summary)
    return summary


def _pick_headline(summary: LogSummary) -> Optional[str]:
    """Return the most useful single-line synopsis of what went wrong.

    dbt failures are checked first because in a Cosmos/dbt environment the
    Airflow ``Task failed with exception`` line is just a wrapper around an
    underlying dbt FAIL/ERROR — the dbt line is what tells you which test
    or model broke.
    """
    if summary.dbt_test_failures:
        return summary.dbt_test_failures[0].line
    if summary.python_exceptions:
        return summary.python_exceptions[0].line
    if summary.python_tracebacks:
        return summary.python_tracebacks[0].line
    if summary.airflow_task_failures:
        return summary.airflow_task_failures[0].line
    if summary.non_zero_exits:
        return summary.non_zero_exits[0].line
    return None
