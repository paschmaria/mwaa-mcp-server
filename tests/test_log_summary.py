"""Tests for log_summary heuristics."""

from awslabs.mwaa_mcp_server.log_summary import summarize_log
from awslabs.mwaa_mcp_server.pagination import extract_log_text


PY_EXC_LOG = """
2026-05-23 - INFO - starting
Traceback (most recent call last):
  File "foo.py", line 5, in <module>
    raise ValueError("something went wrong")
ValueError: something went wrong
Task failed with exception
exit code: 1
"""


def test_python_exception_picked_up() -> None:
    summary = summarize_log(PY_EXC_LOG)
    assert summary.python_tracebacks
    assert summary.python_exceptions
    assert any("ValueError" in h.line for h in summary.python_exceptions)


def test_python_exception_is_headline() -> None:
    # Of the matched lines (traceback, exception, task failed, exit code),
    # the actual exception line is the most diagnostic.
    summary = summarize_log(PY_EXC_LOG)
    assert summary.headline is not None
    assert "ValueError" in summary.headline


def test_airflow_task_failed_marker() -> None:
    summary = summarize_log(PY_EXC_LOG)
    assert summary.airflow_task_failures
    assert any("Task failed" in h.line for h in summary.airflow_task_failures)


def test_non_zero_exit_picked_up() -> None:
    summary = summarize_log("Subprocess returned exit code: 137\n")
    assert summary.non_zero_exits
    assert summary.headline is not None
    assert "137" in summary.headline


def test_empty_log_has_no_headline() -> None:
    summary = summarize_log("")
    assert summary.headline is None


def test_extract_log_text_handles_structured_entries() -> None:
    # Airflow returns logs as a list of structured dicts; extract_log_text
    # must pull out the `event` field per entry.
    raw = {
        "RestApiResponse": {
            "content": [
                {"timestamp": "t1", "level": "info", "event": "first line"},
                {"timestamp": "t2", "level": "info", "event": "\x1b[31msecond line\x1b[0m"},
            ]
        }
    }
    text = extract_log_text(raw)
    lines = text.splitlines()
    assert lines == ["first line", "second line"]


def test_extract_log_text_handles_list_of_strings() -> None:
    raw = {"RestApiResponse": {"content": ["a", "b", "c"]}}
    assert extract_log_text(raw).splitlines() == ["a", "b", "c"]


def test_extract_log_text_handles_plain_string() -> None:
    raw = {"RestApiResponse": "just text"}
    assert extract_log_text(raw) == "just text"
