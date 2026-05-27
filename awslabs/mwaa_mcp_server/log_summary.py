"""Heuristics for extracting the relevant lines from a task log.

Airflow task logs are mostly noise (init/teardown, per-step status output)
with a few signal lines buried inside. This module turns a raw log body
into a short structured summary so the model sees the actual failure cause
rather than the whole 50KB blob.

The patterns here are intentionally framework-agnostic: Python tracebacks,
Python exceptions, Airflow's task-failure markers, and non-zero exit codes.
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
    - Python exceptions (``ValueError: ...``, ``RuntimeError: ...``)
    - Python tracebacks (``Traceback (most recent call last):``)
    - Airflow task-failed markers (``Task failed with exception``)
    - Non-zero exit codes (``exit code: 1``, ``return code 137``)
    """
    lines = log_text.splitlines() or [""]
    summary = LogSummary(headline=None, total_lines=len(lines))

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
    """Return the most useful single-line synopsis of what went wrong."""
    if summary.python_exceptions:
        return summary.python_exceptions[0].line
    if summary.python_tracebacks:
        return summary.python_tracebacks[0].line
    if summary.airflow_task_failures:
        return summary.airflow_task_failures[0].line
    if summary.non_zero_exits:
        return summary.non_zero_exits[0].line
    return None
