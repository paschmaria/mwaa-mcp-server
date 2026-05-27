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


# ---------------------------------------------------------------------------
# dbt extraction
# ---------------------------------------------------------------------------

DBT_TEST_FAIL_LOG = """\
15:09:14  Found 461 models, 27 snapshots, 1906 data tests
15:09:14  Concurrency: 8 threads (target='prod')
15:09:16  1 of 64 START test unique_dim_providers_provider_id ............... [RUN]
15:09:19  1 of 64 FAIL 424288 unique_dim_providers_provider_id .............. [FAIL 424288 in 0.4s]
15:09:32  Failure in test unique_dim_providers_provider_id (models/styleseat_dw/core/_core_models.yml)
15:09:32    Got 424288 results, configured to fail if != 0
15:09:32  Done. PASS=62 WARN=2 ERROR=1 SKIP=0 NO-OP=0 TOTAL=64
Command exited with return code 1
Task failed with exception
"""


def test_dbt_fail_status_line_captured() -> None:
    summary = summarize_log(DBT_TEST_FAIL_LOG)
    # Both the "N of M FAIL ..." status line and the "Failure in test ..."
    # detail line should be captured as dbt test failures.
    assert summary.dbt_test_failures
    matches = [h.line for h in summary.dbt_test_failures]
    assert any("FAIL 424288" in m for m in matches)
    assert any("Failure in test" in m for m in matches)


def test_dbt_failure_wins_over_airflow_marker_in_headline() -> None:
    # The Airflow wrapper "Task failed with exception" comes after the dbt
    # FAIL, but the dbt line is the diagnostic one and should be the headline.
    summary = summarize_log(DBT_TEST_FAIL_LOG)
    assert summary.headline is not None
    assert "FAIL 424288" in summary.headline


def test_dbt_done_stats_parsed() -> None:
    summary = summarize_log(DBT_TEST_FAIL_LOG)
    assert summary.dbt_done_stats == {
        "pass": 62,
        "warn": 2,
        "error": 1,
        "skip": 0,
        "no_op": 0,
        "total": 64,
    }


def test_dbt_done_stats_handles_pre_1_8_logs_without_noop() -> None:
    log = "15:00:00  Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10\n"
    summary = summarize_log(log)
    assert summary.dbt_done_stats == {
        "pass": 10,
        "warn": 0,
        "error": 0,
        "skip": 0,
        "no_op": 0,
        "total": 10,
    }


def test_dbt_runtime_error_captured() -> None:
    log = (
        "15:00:00  Runtime Error in model my_model (models/my_model.sql)\n"
        "15:00:00    Database Error: relation \"foo\" does not exist\n"
        "Task failed with exception\n"
    )
    summary = summarize_log(log)
    assert summary.dbt_test_failures
    assert any("Runtime Error" in h.line for h in summary.dbt_test_failures)


def test_passing_dbt_run_has_no_failures_but_keeps_stats() -> None:
    # A successful dbt run still emits a Done. line — we want stats but no
    # failure hits (and no headline).
    log = (
        "15:00:00  1 of 5 START test foo .................... [RUN]\n"
        "15:00:01  1 of 5 PASS foo .......................... [PASS in 0.5s]\n"
        "15:00:01  Done. PASS=5 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=5\n"
    )
    summary = summarize_log(log)
    assert summary.dbt_test_failures == []
    assert summary.headline is None
    assert summary.dbt_done_stats is not None
    assert summary.dbt_done_stats["error"] == 0


def test_to_dict_exposes_dbt_keys() -> None:
    summary = summarize_log(DBT_TEST_FAIL_LOG)
    d = summary.to_dict()
    # These keys are part of the documented response shape on
    # summarize_task_failure — make sure to_dict actually emits them.
    assert "dbt_test_failures" in d
    assert "dbt_done_stats" in d
    assert isinstance(d["dbt_test_failures"], list)
    assert d["dbt_done_stats"]["error"] == 1


# ---------------------------------------------------------------------------
# CloudWatch envelope stripping
# ---------------------------------------------------------------------------


def test_extract_log_text_strips_cloudwatch_envelope_when_empty() -> None:
    # MWAA's CloudWatch task handler emits the "Reading remote log..." wrapper
    # line plus a literal 'nextForwardToken' even when the underlying stream
    # returned no events (e.g. log retention expired on an old failed run).
    # Without stripping, callers see two meaningless "log" lines that the
    # failure heuristics then try (and fail) to extract a diagnosis from.
    raw = {
        "RestApiResponse": {
            "content": (
                "Reading remote log from Cloudwatch log_group: "
                "arn:aws:logs:us-east-1:123:log-group:airflow-env-Task "
                "log_stream: dag_id=x/run_id=y/task_id=z/attempt=3.log\n"
                "'nextForwardToken'"
            )
        }
    }
    assert extract_log_text(raw) == ""


def test_extract_log_text_keeps_real_log_lines_around_envelope() -> None:
    # The wrapper lines should be stripped without losing real log content
    # interleaved with them.
    raw = {
        "RestApiResponse": {
            "content": (
                "Reading remote log from Cloudwatch log_group: arn:aws:logs:foo\n"
                "[2026-05-27 12:00:01] INFO Starting task\n"
                "Traceback (most recent call last):\n"
                '  File "x.py", line 1, in <module>\n'
                "ValueError: boom\n"
                "'nextForwardToken'"
            )
        }
    }
    out = extract_log_text(raw)
    assert "Reading remote log" not in out
    assert "nextForwardToken" not in out
    assert "ValueError: boom" in out
    assert "Starting task" in out


def test_extract_log_text_strips_fallback_to_local_log_notice() -> None:
    # Airflow also emits "*** Falling back to local log" when CloudWatch
    # lookup fails. That's also envelope, not content.
    raw = {
        "RestApiResponse": {
            "content": (
                "*** Falling back to local log\n"
                "[2026-05-27 12:00:01] INFO real content here\n"
            )
        }
    }
    out = extract_log_text(raw)
    assert "Falling back" not in out
    assert "real content here" in out
