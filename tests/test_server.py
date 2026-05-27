"""Tests for MWAA MCP Server."""

import pytest
from botocore.exceptions import ClientError
from unittest.mock import Mock, patch

from awslabs.mwaa_mcp_server.tools import MWAATools


@pytest.fixture
def mock_boto_client():
    """Mock boto3 MWAA client."""
    with patch("awslabs.mwaa_mcp_server.tools.boto3") as mock_boto:
        mock_client = Mock()
        mock_boto.client.return_value = mock_client
        yield mock_client


@pytest.fixture
def mwaa_tools(mock_boto_client):
    """Create MWAATools instance with mocked client."""
    with patch.dict("os.environ", {"MWAA_MCP_READONLY": "false"}):
        tools = MWAATools()
        tools.mwaa_client = mock_boto_client
        return tools


@pytest.fixture
def readonly_tools(mock_boto_client):
    """Create MWAATools instance in read-only mode."""
    with patch.dict("os.environ", {"MWAA_MCP_READONLY": "true"}):
        tools = MWAATools()
        tools.mwaa_client = mock_boto_client
        return tools


class TestListEnvironments:
    """Test listing MWAA environments."""

    @pytest.mark.asyncio
    async def test_list_environments(self, mwaa_tools, mock_boto_client):
        mock_boto_client.list_environments.return_value = {
            "Environments": ["test-env-1", "test-env-2"]
        }
        mock_boto_client.get_environment.return_value = {
            "Environment": {
                "Name": "test-env-1",
                "Status": "AVAILABLE",
                "Arn": "arn:aws:airflow:us-east-1:123456789012:environment/test-env-1",
                "CreatedAt": "2024-01-01T00:00:00Z",
            }
        }

        result = await mwaa_tools.list_environments()

        assert "Environments" in result
        assert len(result["Environments"]) == 2
        mock_boto_client.list_environments.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_environments_with_max_results(self, mwaa_tools, mock_boto_client):
        mock_boto_client.list_environments.return_value = {"Environments": ["test-env-1"]}
        mock_boto_client.get_environment.return_value = {
            "Environment": {
                "Name": "test-env-1",
                "Status": "AVAILABLE",
                "Arn": "arn:aws:airflow:us-east-1:123456789012:environment/test-env-1",
                "CreatedAt": "2024-01-01T00:00:00Z",
            }
        }

        result = await mwaa_tools.list_environments(max_results=1)

        assert "Environments" in result
        mock_boto_client.list_environments.assert_called_once_with(MaxResults=1)

    @pytest.mark.asyncio
    async def test_list_environments_caps_max_results_at_25(self, mwaa_tools, mock_boto_client):
        mock_boto_client.list_environments.return_value = {"Environments": []}

        await mwaa_tools.list_environments(max_results=100)

        mock_boto_client.list_environments.assert_called_once_with(MaxResults=25)


class TestGetEnvironment:
    """Test getting environment details."""

    @pytest.mark.asyncio
    async def test_get_environment(self, mwaa_tools, mock_boto_client):
        mock_env = {
            "Name": "test-env",
            "Status": "AVAILABLE",
            "Arn": "arn:aws:airflow:us-east-1:123456789012:environment/test-env",
            "WebserverUrl": "https://test.airflow.region.amazonaws.com",
            "ExecutionRoleArn": "arn:aws:iam::123456789012:role/airflow-execution-role",
            "NetworkConfiguration": {
                "SubnetIds": ["subnet-123", "subnet-456"],
                "SecurityGroupIds": ["sg-123"],
            },
        }
        mock_boto_client.get_environment.return_value = {"Environment": mock_env}

        result = await mwaa_tools.get_environment("test-env")

        assert "Environment" in result
        assert result["Environment"]["Name"] == "test-env"
        assert result["Environment"]["Status"] == "AVAILABLE"
        mock_boto_client.get_environment.assert_called_once_with(Name="test-env")

    @pytest.mark.asyncio
    async def test_get_environment_not_found(self, mwaa_tools, mock_boto_client):
        from botocore.exceptions import ClientError

        mock_boto_client.get_environment.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Environment not found"}},
            "GetEnvironment",
        )

        result = await mwaa_tools.get_environment("non-existent")

        assert "error" in result
        assert "ResourceNotFoundException" in result["error"]


class TestReadonlyMode:
    """Test read-only mode restrictions."""

    @pytest.mark.asyncio
    async def test_create_environment_blocked(self, readonly_tools):
        with pytest.raises(PermissionError, match="not allowed in read-only mode"):
            await readonly_tools.create_environment(
                name="test",
                dag_s3_path="s3://bucket/dags",
                execution_role_arn="arn:aws:iam::123456789012:role/test",
                network_configuration={},
                source_bucket_arn="arn:aws:s3:::bucket",
            )

    @pytest.mark.asyncio
    async def test_update_environment_blocked(self, readonly_tools):
        with pytest.raises(PermissionError, match="not allowed in read-only mode"):
            await readonly_tools.update_environment(name="test")

    @pytest.mark.asyncio
    async def test_delete_environment_blocked(self, readonly_tools):
        with pytest.raises(PermissionError, match="not allowed in read-only mode"):
            await readonly_tools.delete_environment("test")

    @pytest.mark.asyncio
    async def test_trigger_dag_run_blocked(self, readonly_tools):
        with pytest.raises(PermissionError, match="not allowed in read-only mode"):
            await readonly_tools.trigger_dag_run("env", "dag_id")


class TestTokenCreation:
    """Test CLI and web login token creation."""

    @pytest.mark.asyncio
    async def test_create_cli_token(self, mwaa_tools, mock_boto_client):
        mock_boto_client.create_cli_token.return_value = {
            "CliToken": "test-token-123",
            "WebServerHostname": "test.airflow.region.amazonaws.com",
        }

        result = await mwaa_tools.create_cli_token("test-env")

        assert result["CliToken"] == "test-token-123"
        assert "WebServerHostname" in result
        mock_boto_client.create_cli_token.assert_called_once_with(Name="test-env")

    @pytest.mark.asyncio
    async def test_create_web_login_token(self, mwaa_tools, mock_boto_client):
        mock_boto_client.create_web_login_token.return_value = {
            "WebToken": "web-token-123",
            "WebServerHostname": "test.airflow.region.amazonaws.com",
            "IamIdentity": "arn:aws:iam::123456789012:user/test",
        }

        result = await mwaa_tools.create_web_login_token("test-env")

        assert result["WebToken"] == "web-token-123"
        assert "WebServerHostname" in result
        assert "IamIdentity" in result


class TestAirflowApiTools:
    """Test Airflow REST API tool wrappers."""

    @pytest.mark.asyncio
    async def test_list_dags(self, mwaa_tools, mock_boto_client):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"dags": [], "total_entries": 0},
            "RestApiStatusCode": 200,
        }

        result = await mwaa_tools.list_dags("test-env")

        mock_boto_client.invoke_rest_api.assert_called_once()
        call_args = mock_boto_client.invoke_rest_api.call_args
        assert call_args.kwargs["Name"] == "test-env"
        assert call_args.kwargs["Method"] == "GET"

    @pytest.mark.asyncio
    async def test_get_dag(self, mwaa_tools, mock_boto_client):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"dag_id": "test_dag"},
            "RestApiStatusCode": 200,
        }

        result = await mwaa_tools.get_dag("test-env", "test_dag")

        call_args = mock_boto_client.invoke_rest_api.call_args
        assert "/dags/test_dag" in call_args.kwargs["Path"]

    @pytest.mark.asyncio
    async def test_trigger_dag_run(self, mwaa_tools, mock_boto_client):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"dag_run_id": "manual__test"},
            "RestApiStatusCode": 200,
        }

        result = await mwaa_tools.trigger_dag_run("test-env", "test_dag", dag_run_id="manual__test")

        call_args = mock_boto_client.invoke_rest_api.call_args
        assert call_args.kwargs["Method"] == "POST"
        assert "/dags/test_dag/dagRuns" in call_args.kwargs["Path"]

    @pytest.mark.asyncio
    async def test_get_task_logs(self, mwaa_tools, mock_boto_client):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"content": "log output"},
            "RestApiStatusCode": 200,
        }

        result = await mwaa_tools.get_task_logs("test-env", "dag1", "run1", "task1", 2)

        call_args = mock_boto_client.invoke_rest_api.call_args
        assert "/logs/2" in call_args.kwargs["Path"]

    @pytest.mark.asyncio
    async def test_get_task_logs_defaults_to_try_1(self, mwaa_tools, mock_boto_client):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"content": "log output"},
            "RestApiStatusCode": 200,
        }

        result = await mwaa_tools.get_task_logs("test-env", "dag1", "run1", "task1")

        call_args = mock_boto_client.invoke_rest_api.call_args
        assert "/logs/1" in call_args.kwargs["Path"]

    @pytest.mark.asyncio
    async def test_list_connections(self, mwaa_tools, mock_boto_client):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"connections": []},
            "RestApiStatusCode": 200,
        }

        result = await mwaa_tools.list_connections("test-env")

        call_args = mock_boto_client.invoke_rest_api.call_args
        assert "/connections" in call_args.kwargs["Path"]

    @pytest.mark.asyncio
    async def test_list_variables(self, mwaa_tools, mock_boto_client):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"variables": []},
            "RestApiStatusCode": 200,
        }

        result = await mwaa_tools.list_variables("test-env")

        call_args = mock_boto_client.invoke_rest_api.call_args
        assert "/variables" in call_args.kwargs["Path"]

    @pytest.mark.asyncio
    async def test_get_import_errors(self, mwaa_tools, mock_boto_client):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"import_errors": []},
            "RestApiStatusCode": 200,
        }

        result = await mwaa_tools.get_import_errors("test-env")

        call_args = mock_boto_client.invoke_rest_api.call_args
        path = call_args.kwargs["Path"]
        # Airflow 3.x: top-level /importErrors (not /dags/importErrors,
        # which was being parsed as /dags/{dag_id="importErrors"}).
        assert path.startswith("/importErrors")
        assert "/dags/importErrors" not in path


class TestAirflowApiErrorSurfacing:
    """When the underlying Airflow REST call fails, callers should see what
    really happened — status code and response body — not an opaque empty
    ``RestApiClientException``.
    """

    @pytest.mark.asyncio
    async def test_client_error_surfaces_status_and_body(
        self, mwaa_tools, mock_boto_client
    ):
        # Simulate MWAA raising RestApiClientException with the Airflow body
        # tucked into the response payload. boto exposes it via e.response.
        airflow_body = {"detail": "DAG with dag_id: foo was not found"}
        err = ClientError(
            error_response={
                "Error": {
                    "Code": "RestApiClientException",
                    "Message": "",
                },
                "RestApiStatusCode": 404,
                "RestApiResponse": airflow_body,
            },
            operation_name="InvokeRestApi",
        )
        mock_boto_client.invoke_rest_api.side_effect = err

        result = await mwaa_tools.get_dag("test-env", "missing-dag")

        # Previously this returned only {"error": "<empty boto wrapper text>"}
        # which was useless for diagnosis. Now the real Airflow status code
        # and body should be reachable.
        assert result.get("error_code") == "RestApiClientException"
        assert result.get("rest_api_status_code") == 404
        assert result.get("rest_api_response") == airflow_body

    @pytest.mark.asyncio
    async def test_client_error_with_empty_message_still_has_status(
        self, mwaa_tools, mock_boto_client
    ):
        # The original opaque-error case: empty Message, no body. The status
        # code is still informative, so callers can at least tell 4xx vs 5xx.
        err = ClientError(
            error_response={
                "Error": {"Code": "RestApiClientException", "Message": ""},
                "RestApiStatusCode": 422,
            },
            operation_name="InvokeRestApi",
        )
        mock_boto_client.invoke_rest_api.side_effect = err

        result = await mwaa_tools.get_dag_source("test-env", "some-dag")
        assert result.get("rest_api_status_code") == 422
        assert result.get("error_code") == "RestApiClientException"


class TestListRecentFailures:
    """list_recent_failures must actually return *recent* failures.

    Airflow's batch task-instances endpoint silently ignores ``start_date_gte``
    and orders oldest-first, so we have to filter and sort client-side. The
    regression these tests guard against: returning months-old failures when
    the caller asked for the last 3 days.
    """

    @pytest.mark.asyncio
    async def test_task_instance_mode_filters_old_failures(
        self, mwaa_tools, mock_boto_client
    ):
        # Mix of old (Feb) and recent (within lookback) task instances.
        # The Feb ones should be filtered out; the recent ones should be
        # ordered newest-first.
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {
                "task_instances": [
                    {
                        "task_id": "ancient_task",
                        "dag_id": "old_dag",
                        "dag_run_id": "scheduled__2026-02-22T00:35:00+00:00",
                        "start_date": "2026-02-22T00:55:31Z",
                        "state": "failed",
                    },
                    {
                        "task_id": "recent_task_old",
                        "dag_id": "x",
                        "dag_run_id": "scheduled__2026-05-25T00:00:00+00:00",
                        "start_date": "2026-05-25T01:00:00Z",
                        "state": "failed",
                    },
                    {
                        "task_id": "recent_task_new",
                        "dag_id": "x",
                        "dag_run_id": "scheduled__2026-05-27T00:00:00+00:00",
                        "start_date": "2026-05-27T01:00:00Z",
                        "state": "failed",
                    },
                ],
                "total_entries": 3,
            },
            "RestApiStatusCode": 200,
        }

        # Freeze "now" so the cutoff (now - days) is deterministic.
        import datetime as _dt

        class _FixedDT(_dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return _dt.datetime(2026, 5, 27, 12, 0, 0, tzinfo=tz)

        with patch("awslabs.mwaa_mcp_server.tools.datetime", _FixedDT):
            result = await mwaa_tools.list_recent_failures(
                environment_name="test-env",
                days=3,  # cutoff = 2026-05-24T12:00:00Z
            )

        tis = result.get("task_instances") or []
        ids = [t["task_id"] for t in tis]
        # Ancient task must be filtered out
        assert "ancient_task" not in ids
        # Both recent ones must be present, newest first
        assert ids == ["recent_task_new", "recent_task_old"]
        # Summary should expose what we did
        summary = result.get("summary", {})
        assert summary.get("client_filtered_count") == 2
        assert summary.get("client_sorted") == "start_date desc"

    @pytest.mark.asyncio
    async def test_dag_id_mode_filters_old_failures(
        self, mwaa_tools, mock_boto_client
    ):
        # When dag_id is passed we use the dag_runs endpoint; same kind of
        # client-side filter applies. Existing code already had it for this
        # branch — this test guards against future regressions.
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {
                "dag_runs": [
                    {
                        "dag_run_id": "scheduled__2026-02-22T00:35:00+00:00",
                        "start_date": "2026-02-22T00:55:31Z",
                        "state": "failed",
                    },
                    {
                        "dag_run_id": "scheduled__2026-05-26T00:00:00+00:00",
                        "start_date": "2026-05-26T01:00:00Z",
                        "state": "failed",
                    },
                ],
                "total_entries": 2,
            },
            "RestApiStatusCode": 200,
        }

        import datetime as _dt

        class _FixedDT(_dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return _dt.datetime(2026, 5, 27, 12, 0, 0, tzinfo=tz)

        with patch("awslabs.mwaa_mcp_server.tools.datetime", _FixedDT):
            result = await mwaa_tools.list_recent_failures(
                environment_name="test-env",
                dag_id="my_dag",
                days=3,
            )

        runs = result.get("dag_runs") or []
        ids = [r["dag_run_id"] for r in runs]
        assert "scheduled__2026-02-22T00:35:00+00:00" not in ids
        assert "scheduled__2026-05-26T00:00:00+00:00" in ids


class TestGetDagRunHeatmap:
    """The heatmap tool must filter cells client-side because Airflow's
    batch task-instances endpoint silently ignores ``start_date_gte``.

    Same bug class as list_recent_failures task-instance mode — the
    regression these tests guard against is returning months-old cells when
    the caller asked for the last N days.
    """

    @pytest.mark.asyncio
    async def test_filters_task_instances_outside_window(
        self, mwaa_tools, mock_boto_client
    ):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {
                "task_instances": [
                    # Way outside the 3-day window.
                    {
                        "task_id": "t1",
                        "dag_run_id": "scheduled__2026-02-22T00:00:00+00:00",
                        "logical_date": "2026-02-22T00:00:00Z",
                        "start_date": "2026-02-22T01:00:00Z",
                        "state": "failed",
                        "try_number": 1,
                    },
                    # Inside the window.
                    {
                        "task_id": "t1",
                        "dag_run_id": "scheduled__2026-05-26T00:00:00+00:00",
                        "logical_date": "2026-05-26T00:00:00Z",
                        "start_date": "2026-05-26T01:00:00Z",
                        "state": "success",
                        "try_number": 1,
                    },
                ]
            },
            "RestApiStatusCode": 200,
        }

        import datetime as _dt

        class _FixedDT(_dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return _dt.datetime(2026, 5, 27, 12, 0, 0, tzinfo=tz)

        with patch("awslabs.mwaa_mcp_server.tools.datetime", _FixedDT):
            result = await mwaa_tools.get_dag_run_heatmap(
                "test-env", "my_dag", days=3
            )

        dates = result["execution_dates"]
        # Out-of-window date must not appear; in-window date must.
        assert "2026-02-22" not in dates
        assert "2026-05-26" in dates
        # And the summary should expose the cutoff so callers can see what
        # window the data covers.
        assert result["summary"]["client_filtered_cutoff_date"] == "2026-05-24"

    @pytest.mark.asyncio
    async def test_includes_failed_before_start_via_derived_date(
        self, mwaa_tools, mock_boto_client
    ):
        # Failed task instances that never started (queued -> failed) have
        # null start_date. _derive_execution_date falls back to logical_date
        # / run_after / dag_run_id prefix, so they still get included if
        # their derived date is in the window.
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {
                "task_instances": [
                    {
                        "task_id": "queued_then_failed",
                        "dag_run_id": "scheduled__2026-05-26T00:00:00+00:00",
                        "logical_date": "2026-05-26T00:00:00Z",
                        "start_date": None,
                        "state": "failed",
                        "try_number": 1,
                    },
                ]
            },
            "RestApiStatusCode": 200,
        }

        import datetime as _dt

        class _FixedDT(_dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return _dt.datetime(2026, 5, 27, 12, 0, 0, tzinfo=tz)

        with patch("awslabs.mwaa_mcp_server.tools.datetime", _FixedDT):
            result = await mwaa_tools.get_dag_run_heatmap(
                "test-env", "my_dag", days=3
            )

        # The null-start_date failure should still be present because its
        # logical_date is inside the window.
        assert "2026-05-26" in result["execution_dates"]
        assert "queued_then_failed" in result["task_ids"]


class TestGetDagSourceTwoStep:
    """Airflow 3.x removed ``/dags/{dag_id}/dagSource`` (returns 404 "API
    route not found"). The replacement is a two-step flow:

    1. ``GET /dags/{dag_id}`` returns a ``file_token`` for the DAG file
    2. ``GET /dagSources/{file_token}`` returns the source

    These tests guard against regressing to the broken single-call form.
    """

    @pytest.mark.asyncio
    async def test_two_step_flow(self, mwaa_tools, mock_boto_client):
        # First call to /dags/{dag_id} returns file_token; second call to
        # /dagSources/{file_token} returns source text.
        mock_boto_client.invoke_rest_api.side_effect = [
            {
                "RestApiResponse": {
                    "dag_id": "my_dag",
                    "file_token": "ABCDEF_token_xyz",
                },
                "RestApiStatusCode": 200,
            },
            {
                "RestApiResponse": {"content": "from airflow import DAG\n..."},
                "RestApiStatusCode": 200,
            },
        ]

        result = await mwaa_tools.get_dag_source("test-env", "my_dag")

        # Two boto calls in order.
        assert mock_boto_client.invoke_rest_api.call_count == 2
        first_path = mock_boto_client.invoke_rest_api.call_args_list[0].kwargs["Path"]
        second_path = mock_boto_client.invoke_rest_api.call_args_list[1].kwargs["Path"]
        assert first_path == "/dags/my_dag"
        assert second_path == "/dagSources/ABCDEF_token_xyz"
        # Second call's response is what get_dag_source returns.
        assert result.get("RestApiResponse", {}).get("content", "").startswith("from airflow")
        # Old broken path must not appear anywhere.
        all_paths = [
            c.kwargs["Path"]
            for c in mock_boto_client.invoke_rest_api.call_args_list
        ]
        assert not any("/dagSource" in p and "/dagSources/" not in p for p in all_paths)

    @pytest.mark.asyncio
    async def test_missing_file_token_returns_error_without_second_call(
        self, mwaa_tools, mock_boto_client
    ):
        # If Airflow's /dags/{dag_id} response doesn't include a file_token
        # (shouldn't happen in 3.x, but guard against it), we must not call
        # /dagSources/None — return an explicit error instead.
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"dag_id": "my_dag"},  # no file_token
            "RestApiStatusCode": 200,
        }

        result = await mwaa_tools.get_dag_source("test-env", "my_dag")

        assert "error" in result
        assert "file_token" in result["error"]
        # Only the first call should have happened.
        assert mock_boto_client.invoke_rest_api.call_count == 1

    @pytest.mark.asyncio
    async def test_propagates_first_call_error(self, mwaa_tools, mock_boto_client):
        # If /dags/{dag_id} errors (e.g. DAG doesn't exist), bubble that up
        # rather than trying the second call against an unknown file_token.
        mock_boto_client.invoke_rest_api.side_effect = ClientError(
            error_response={
                "Error": {"Code": "RestApiClientException", "Message": ""},
                "RestApiStatusCode": 404,
                "RestApiResponse": {"detail": "DAG not found"},
            },
            operation_name="InvokeRestApi",
        )

        result = await mwaa_tools.get_dag_source("test-env", "missing_dag")

        assert result.get("rest_api_status_code") == 404
        # Only one call attempted.
        assert mock_boto_client.invoke_rest_api.call_count == 1


class TestListTaskInstancesOrderBy:
    """``list_task_instances`` must default to newest-first ordering.

    Airflow's batch task-instances endpoint defaults to oldest-first when
    no ``order_by`` is sent, which made "what failed in the last 3 days?"
    queries return months-old rows. The fix sends ``order_by=-start_date``
    by default; this test guards against regressing.
    """

    @pytest.mark.asyncio
    async def test_default_order_by_is_start_date_desc(
        self, mwaa_tools, mock_boto_client
    ):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"task_instances": [], "total_entries": 0},
            "RestApiStatusCode": 200,
        }

        await mwaa_tools.list_task_instances(environment_name="test-env")

        path = mock_boto_client.invoke_rest_api.call_args.kwargs["Path"]
        # The order_by gets URL-encoded into the query string.
        assert "order_by=-start_date" in path

    @pytest.mark.asyncio
    async def test_explicit_order_by_is_respected(
        self, mwaa_tools, mock_boto_client
    ):
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"task_instances": [], "total_entries": 0},
            "RestApiStatusCode": 200,
        }

        await mwaa_tools.list_task_instances(
            environment_name="test-env", order_by="end_date"
        )

        path = mock_boto_client.invoke_rest_api.call_args.kwargs["Path"]
        assert "order_by=end_date" in path
        assert "order_by=-start_date" not in path

    @pytest.mark.asyncio
    async def test_list_recent_failures_ti_mode_passes_order_by(
        self, mwaa_tools, mock_boto_client
    ):
        # Without a dag_id, list_recent_failures goes through the batch
        # task-instances endpoint. It must push order_by=-start_date so
        # Airflow returns the recent rows, not the first N oldest.
        mock_boto_client.invoke_rest_api.return_value = {
            "RestApiResponse": {"task_instances": [], "total_entries": 0},
            "RestApiStatusCode": 200,
        }

        await mwaa_tools.list_recent_failures(
            environment_name="test-env", days=3
        )

        path = mock_boto_client.invoke_rest_api.call_args.kwargs["Path"]
        assert "order_by=-start_date" in path
