"""MWAA MCP Server Tools - Implementation of MWAA operations."""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .log_summary import summarize_log
from .pagination import (
    DEFAULT_PAGE_SIZE,
    encode_log_resource_uri,
    extract_log_text,
    truncate_log_text,
    with_resource_link,
)

logger = logging.getLogger(__name__)

# Terminal task/run states considered "done failing" for diagnostic tools.
FAILURE_STATES = {"failed", "upstream_failed"}


def _derive_execution_date(ti: Dict[str, Any]) -> str:
    """Return a YYYY-MM-DD string for the run this task instance belongs to.

    Airflow 3.x removed ``execution_date``/``logical_date`` from the task
    instance payload. Fall back to ``start_date`` and finally to parsing the
    timestamp out of the ``dag_run_id`` (e.g. ``scheduled__2026-05-22T...``
    or ``asset_triggered__2026-05-23T...``).
    """
    for key in ("logical_date", "execution_date", "start_date", "run_after"):
        v = ti.get(key)
        if isinstance(v, str) and len(v) >= 10:
            return v[:10]
    run_id = ti.get("dag_run_id") or ""
    if "__" in run_id:
        suffix = run_id.split("__", 1)[1]
        if len(suffix) >= 10 and suffix[4] == "-" and suffix[7] == "-":
            return suffix[:10]
    return ""


def _sanitize_mermaid_id(node_id: str) -> str:
    """Mermaid node IDs can't contain `.`, `-`, or spaces — replace with `_`."""
    return "".join(c if c.isalnum() else "_" for c in node_id)


def _build_mermaid(
    dag_id: str, nodes: List[Dict[str, Any]], edges: List[Dict[str, str]]
) -> str:
    """Build a Mermaid `flowchart LR` string for a DAG's task graph."""
    lines = [f"flowchart LR", f"  %% DAG: {dag_id}"]
    for n in nodes:
        nid = _sanitize_mermaid_id(n["id"])
        label = n["id"].replace('"', "'")
        op = n.get("operator") or ""
        suffix = f"\\n[{op}]" if op else ""
        lines.append(f'  {nid}["{label}{suffix}"]')
    for e in edges:
        a = _sanitize_mermaid_id(e["from"])
        b = _sanitize_mermaid_id(e["to"])
        lines.append(f"  {a} --> {b}")
    return "\n".join(lines)


class MWAATools:
    """Tools for interacting with Amazon MWAA."""

    def __init__(self) -> None:
        """Initialize MWAA tools with AWS clients."""
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.readonly = os.getenv("MWAA_MCP_READONLY", "true").lower() == "true"

        self.mwaa_client = boto3.client("mwaa", region_name=self.region)

        logger.info("Initialized MWAA tools for region: %s", self.region)
        if self.readonly:
            logger.info("Running in read-only mode")

    def _check_readonly(self, operation: str) -> None:
        """Check if operation is allowed in read-only mode."""
        if self.readonly:
            raise PermissionError(f"Operation '{operation}' not allowed in read-only mode")

    def _invoke_airflow_api(
        self, environment_name: str, method: str, path: str, **kwargs: Any
    ) -> Dict[str, Any]:
        """Invoke Airflow REST API using MWAA client."""
        try:
            params: Dict[str, Any] = {
                "Name": environment_name,
                "Method": method.upper(),
                "Path": path,
            }

            if "params" in kwargs:
                from urllib.parse import quote as _q

                pairs: List[str] = []
                for k, v in kwargs["params"].items():
                    if v is None:
                        continue
                    # Coerce simple string -> typed value so Airflow's strict
                    # validators (especially Airflow 3) accept it.
                    if isinstance(v, str) and v.isdigit():
                        v = int(v)
                    elif isinstance(v, str) and v.lower() in ("true", "false"):
                        v = v.lower() == "true"
                    elif (
                        isinstance(v, str)
                        and v.startswith("[")
                        and v.endswith("]")
                    ):
                        try:
                            v = json.loads(v)
                        except json.JSONDecodeError:
                            pass
                    # Lists must be emitted as repeated params (`state=a&state=b`),
                    # not as the str() of a Python list.
                    if isinstance(v, (list, tuple)):
                        for item in v:
                            pairs.append(f"{_q(str(k))}={_q(str(item))}")
                    elif isinstance(v, bool):
                        pairs.append(f"{_q(str(k))}={'true' if v else 'false'}")
                    else:
                        pairs.append(f"{_q(str(k))}={_q(str(v))}")

                if pairs:
                    params["Path"] = f"{path}?{'&'.join(pairs)}"

            if "json_data" in kwargs:
                params["Body"] = json.dumps(kwargs["json_data"])

            response = self.mwaa_client.invoke_rest_api(**params)
            return response

        except ClientError as e:
            # MWAA's RestApiClientException / RestApiServerException are
            # ClientError subclasses. The boto wrapper's str() is often empty
            # (e.g. "An error occurred (RestApiClientException) ... : ").
            # The actual diagnostic info lives in e.response — surface the
            # HTTP status code from Airflow and any response body so callers
            # can see what really failed (404 for a removed endpoint, 422 for
            # a bad param, etc.) instead of getting an opaque empty error.
            err_info = e.response.get("Error") or {}
            err_code = err_info.get("Code")
            err_msg = err_info.get("Message") or ""
            rest_status = e.response.get("RestApiStatusCode")
            rest_body = e.response.get("RestApiResponse")
            logger.error(
                "Error invoking Airflow API %s %s: code=%s msg=%s status=%s body=%s",
                method, path, err_code, err_msg, rest_status, rest_body,
            )
            return {
                "error": err_msg or str(e),
                "error_code": err_code,
                "rest_api_status_code": rest_status,
                "rest_api_response": rest_body,
            }
        except Exception as e:
            logger.error("Error invoking Airflow API %s %s: %s", method, path, e)
            return {"error": str(e)}

    # Environment Management Methods
    async def list_environments(self, max_results: Optional[int] = None) -> Dict[str, Any]:
        """List MWAA environments."""
        try:
            kwargs: Dict[str, Any] = {}
            if max_results:
                kwargs["MaxResults"] = min(max_results, 25)

            response = self.mwaa_client.list_environments(**kwargs)

            environments = []
            for env_name in response.get("Environments", []):
                try:
                    env_details = await self.get_environment(env_name)
                    environments.append(
                        {
                            "Name": env_name,
                            "Status": env_details.get("Environment", {}).get("Status"),
                            "Arn": env_details.get("Environment", {}).get("Arn"),
                            "CreatedAt": env_details.get("Environment", {}).get("CreatedAt"),
                        }
                    )
                except Exception as e:
                    logger.error("Error getting details for environment %s: %s", env_name, e)
                    environments.append(
                        {
                            "Name": env_name,
                            "Status": "ERROR",
                            "Error": str(e),
                        }
                    )

            return {
                "Environments": environments,
                "NextToken": response.get("NextToken"),
            }

        except (ClientError, BotoCoreError) as e:
            logger.error("Error listing environments: %s", e)
            return {"error": str(e)}

    async def get_environment(self, name: str) -> Dict[str, Any]:
        """Get environment details."""
        try:
            response = self.mwaa_client.get_environment(Name=name)

            env = response.get("Environment", {})
            if "CreatedAt" in env:
                env["CreatedAt"] = env["CreatedAt"].isoformat()
            if "LastUpdate" in env and "CreatedAt" in env["LastUpdate"]:
                env["LastUpdate"]["CreatedAt"] = env["LastUpdate"]["CreatedAt"].isoformat()

            return {"Environment": env}

        except (ClientError, BotoCoreError) as e:
            logger.error("Error getting environment %s: %s", name, e)
            return {"error": str(e)}

    async def create_environment(self, **kwargs: Any) -> Dict[str, Any]:
        """Create a new MWAA environment."""
        self._check_readonly("create_environment")

        try:
            params = {k: v for k, v in kwargs.items() if v is not None}

            boto_params: Dict[str, Any] = {}
            param_mapping = {
                "name": "Name",
                "dag_s3_path": "DagS3Path",
                "execution_role_arn": "ExecutionRoleArn",
                "network_configuration": "NetworkConfiguration",
                "source_bucket_arn": "SourceBucketArn",
                "airflow_version": "AirflowVersion",
                "environment_class": "EnvironmentClass",
                "max_workers": "MaxWorkers",
                "min_workers": "MinWorkers",
                "schedulers": "Schedulers",
                "webserver_access_mode": "WebserverAccessMode",
                "weekly_maintenance_window_start": "WeeklyMaintenanceWindowStart",
                "tags": "Tags",
                "airflow_configuration_options": "AirflowConfigurationOptions",
                "logging_configuration": "LoggingConfiguration",
                "requirements_s3_path": "RequirementsS3Path",
                "plugins_s3_path": "PluginsS3Path",
                "startup_script_s3_path": "StartupScriptS3Path",
            }

            for snake_key, value in params.items():
                if snake_key in param_mapping:
                    boto_params[param_mapping[snake_key]] = value

            response = self.mwaa_client.create_environment(**boto_params)
            return {"Arn": response["Arn"]}

        except (ClientError, BotoCoreError) as e:
            logger.error("Error creating environment: %s", e)
            return {"error": str(e)}

    async def update_environment(self, **kwargs: Any) -> Dict[str, Any]:
        """Update an existing MWAA environment."""
        self._check_readonly("update_environment")

        try:
            params = {k: v for k, v in kwargs.items() if v is not None}

            boto_params: Dict[str, Any] = {}
            param_mapping = {
                "name": "Name",
                "dag_s3_path": "DagS3Path",
                "execution_role_arn": "ExecutionRoleArn",
                "network_configuration": "NetworkConfiguration",
                "source_bucket_arn": "SourceBucketArn",
                "airflow_version": "AirflowVersion",
                "environment_class": "EnvironmentClass",
                "max_workers": "MaxWorkers",
                "min_workers": "MinWorkers",
                "schedulers": "Schedulers",
                "webserver_access_mode": "WebserverAccessMode",
                "weekly_maintenance_window_start": "WeeklyMaintenanceWindowStart",
                "airflow_configuration_options": "AirflowConfigurationOptions",
                "logging_configuration": "LoggingConfiguration",
                "requirements_s3_path": "RequirementsS3Path",
                "plugins_s3_path": "PluginsS3Path",
                "startup_script_s3_path": "StartupScriptS3Path",
            }

            for snake_key, value in params.items():
                if snake_key in param_mapping:
                    boto_params[param_mapping[snake_key]] = value

            response = self.mwaa_client.update_environment(**boto_params)
            return {"Arn": response["Arn"]}

        except (ClientError, BotoCoreError) as e:
            logger.error("Error updating environment: %s", e)
            return {"error": str(e)}

    async def delete_environment(self, name: str) -> Dict[str, Any]:
        """Delete an existing MWAA environment."""
        self._check_readonly("delete_environment")

        try:
            self.mwaa_client.delete_environment(Name=name)
            return {"message": f"Environment {name} deleted successfully"}

        except (ClientError, BotoCoreError) as e:
            logger.error("Error deleting environment %s: %s", name, e)
            return {"error": str(e)}

    async def create_cli_token(self, name: str) -> Dict[str, Any]:
        """Create a CLI token for the environment."""
        try:
            response = self.mwaa_client.create_cli_token(Name=name)
            return {
                "CliToken": response["CliToken"],
                "WebServerHostname": response["WebServerHostname"],
            }

        except (ClientError, BotoCoreError) as e:
            logger.error("Error creating CLI token for %s: %s", name, e)
            return {"error": str(e)}

    async def create_web_login_token(self, name: str) -> Dict[str, Any]:
        """Create a web login token for the environment."""
        try:
            response = self.mwaa_client.create_web_login_token(Name=name)
            return {
                "WebToken": response["WebToken"],
                "WebServerHostname": response["WebServerHostname"],
                "IamIdentity": response["IamIdentity"],
            }

        except (ClientError, BotoCoreError) as e:
            logger.error("Error creating web login token for %s: %s", name, e)
            return {"error": str(e)}

    # Airflow API Methods
    async def list_dags(
        self,
        environment_name: str,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        tags: Optional[List[str]] = None,
        dag_id_pattern: Optional[str] = None,
        only_active: Optional[bool] = True,
    ) -> Dict[str, Any]:
        """List DAGs via Airflow API."""
        params: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "only_active": only_active,
        }

        if tags:
            params["tags"] = ",".join(tags)
        if dag_id_pattern:
            params["dag_id_pattern"] = dag_id_pattern

        return self._invoke_airflow_api(environment_name, "GET", "/dags", params=params)

    async def get_dag(self, environment_name: str, dag_id: str) -> Dict[str, Any]:
        """Get DAG details via Airflow API."""
        return self._invoke_airflow_api(environment_name, "GET", f"/dags/{dag_id}")

    async def get_dag_source(self, environment_name: str, dag_id: str) -> Dict[str, Any]:
        """Get DAG source code via Airflow API."""
        return self._invoke_airflow_api(
            environment_name, "GET", f"/dags/{dag_id}/dagSource"
        )

    async def trigger_dag_run(
        self,
        environment_name: str,
        dag_id: str,
        dag_run_id: Optional[str] = None,
        conf: Optional[Dict[str, Any]] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Trigger a DAG run via Airflow API."""
        self._check_readonly("trigger_dag_run")

        data: Dict[str, Any] = {}

        if dag_run_id:
            data["dag_run_id"] = dag_run_id
        else:
            data["dag_run_id"] = f"manual__{datetime.now(timezone.utc).isoformat()}"

        if conf:
            data["conf"] = conf
        if note:
            data["note"] = note

        return self._invoke_airflow_api(
            environment_name, "POST", f"/dags/{dag_id}/dagRuns", json_data=data
        )

    async def get_dag_run(
        self, environment_name: str, dag_id: str, dag_run_id: str
    ) -> Dict[str, Any]:
        """Get DAG run details via Airflow API."""
        return self._invoke_airflow_api(
            environment_name, "GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}"
        )

    async def list_dag_runs(
        self,
        environment_name: str,
        dag_id: str,
        limit: Optional[int] = 100,
        state: Optional[List[str]] = None,
        execution_date_gte: Optional[str] = None,
        execution_date_lte: Optional[str] = None,
        order_by: Optional[str] = "-start_date",
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Dict[str, Any]:
        """List DAG runs via Airflow API.

        Defaults to newest-first via ``order_by="-start_date"`` and returns a
        paginated envelope with a follow-up ``next_resource_uri`` instead of
        dumping every run.
        """
        params: Dict[str, Any] = {"limit": limit}
        if state:
            params["state"] = state
        if execution_date_gte:
            params["execution_date_gte"] = execution_date_gte
        if execution_date_lte:
            params["execution_date_lte"] = execution_date_lte
        if order_by:
            params["order_by"] = order_by

        raw = self._invoke_airflow_api(
            environment_name, "GET", f"/dags/{dag_id}/dagRuns", params=params
        )
        if "error" in raw:
            return raw

        runs = (raw.get("RestApiResponse") or {}).get("dag_runs", []) or []
        total = (raw.get("RestApiResponse") or {}).get("total_entries", len(runs))

        return with_resource_link(
            summary={
                "dag_id": dag_id,
                "environment_name": environment_name,
                "total_entries_reported_by_airflow": total,
                "order_by": order_by,
                "filters": {
                    "state": state,
                    "execution_date_gte": execution_date_gte,
                    "execution_date_lte": execution_date_lte,
                },
            },
            items=runs,
            page=page,
            page_size=page_size,
            resource_path=f"dag_runs/{environment_name}/{dag_id}",
            resource_params={
                "limit": limit,
                "order_by": order_by,
                "state": state,
                "execution_date_gte": execution_date_gte,
                "execution_date_lte": execution_date_lte,
            },
            item_key="dag_runs",
        )

    async def get_task_instance(
        self, environment_name: str, dag_id: str, dag_run_id: str, task_id: str
    ) -> Dict[str, Any]:
        """Get task instance details via Airflow API."""
        return self._invoke_airflow_api(
            environment_name,
            "GET",
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}",
        )

    async def get_task_logs(
        self,
        environment_name: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        task_try_number: Optional[int] = None,
        full: bool = False,
    ) -> Dict[str, Any]:
        """Get task logs via Airflow API.

        Returns head+tail of the log inline by default plus a ``resource_uri``
        pointing at the full body. Pass ``full=True`` to bypass truncation —
        the resource handler uses this path.
        """
        if task_try_number is None:
            task_try_number = 1

        endpoint = (
            f"/dags/{dag_id}/dagRuns/{dag_run_id}"
            f"/taskInstances/{task_id}/logs/{task_try_number}"
        )
        raw = self._invoke_airflow_api(environment_name, "GET", endpoint)
        if "error" in raw:
            return raw

        log_text = extract_log_text(raw)
        if full:
            return {"log_text": log_text, "total_lines": len(log_text.splitlines())}

        body, meta = truncate_log_text(log_text)
        return {
            "summary": {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "task_id": task_id,
                "task_try_number": task_try_number,
                **meta,
            },
            "log_text": body,
            "resource_uri": encode_log_resource_uri(
                environment_name, dag_id, dag_run_id, task_id, task_try_number
            ),
        }

    async def get_dag_run_heatmap(
        self,
        environment_name: str,
        dag_id: str,
        days: int = 14,
    ) -> Dict[str, Any]:
        """Build a (task_id × execution_date) heatmap of run states.

        Pulls task instances for the DAG over the last ``days`` window via
        the batch endpoint, collapses to one cell per
        ``(task_id, execution_date)`` (taking the most recent try), and
        returns:

        - ``task_ids``: deduped, alphabetically ordered
        - ``execution_dates``: deduped, ascending ISO dates
        - ``cells``: [{task_id, execution_date, state, dag_run_id, ...}]

        Hosts with MCP Apps support render this as a clickable grid; text
        hosts can scan ``cells`` directly.
        """
        # Airflow's REST API wants ISO with 'Z' (not '+00:00') and without
        # microseconds — match its examples.
        cutoff = (
            (datetime.now(timezone.utc) - timedelta(days=days))
            .replace(microsecond=0)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        # Fetch with a high limit; this endpoint returns up to 1000 per page.
        # Airflow 3 removed execution_date/logical_date from task instances —
        # use start_date_gte and derive the calendar date from start_date or
        # the run id prefix.
        params: Dict[str, Any] = {
            "limit": 1000,
            "start_date_gte": cutoff,
        }
        endpoint = f"/dags/{dag_id}/dagRuns/~/taskInstances"
        raw = self._invoke_airflow_api(
            environment_name, "GET", endpoint, params=params
        )
        if "error" in raw:
            return raw

        body = raw.get("RestApiResponse") or {}
        instances = body.get("task_instances", []) or []

        # Airflow's batch task-instances endpoint silently ignores
        # ``start_date_gte`` (same bug class as list_recent_failures).
        # Without a client-side filter the heatmap returns the full
        # history of the DAG instead of the requested window — e.g.
        # ``days=7`` came back with cells from three months ago.
        # ``_derive_execution_date`` is used rather than ``start_date``
        # because failed-before-start task instances (queued -> failed)
        # have null start_date and would be excluded entirely; the
        # helper falls back to logical_date / run_after / parsing the
        # dag_run_id prefix.
        cutoff_date = cutoff[:10]  # "YYYY-MM-DD"
        instances = [
            ti for ti in instances
            if _derive_execution_date(ti) >= cutoff_date
        ]

        # Collapse to most-recent-try per (task_id, execution_date).
        best: Dict[tuple, Dict[str, Any]] = {}
        for ti in instances:
            tid = ti.get("task_id")
            if not tid:
                continue
            exec_date = _derive_execution_date(ti)
            if not exec_date:
                continue
            key = (tid, exec_date)
            try_no = ti.get("try_number") or 0
            existing = best.get(key)
            if existing is None or (existing.get("try_number") or 0) < try_no:
                best[key] = ti

        cells: List[Dict[str, Any]] = []
        for (tid, exec_date), ti in best.items():
            cells.append(
                {
                    "task_id": tid,
                    "execution_date": exec_date,
                    "state": ti.get("state"),
                    "dag_run_id": ti.get("dag_run_id"),
                    "try_number": ti.get("try_number"),
                    "duration": ti.get("duration"),
                }
            )

        task_ids = sorted({c["task_id"] for c in cells})
        execution_dates = sorted({c["execution_date"] for c in cells})

        return {
            "summary": {
                "environment_name": environment_name,
                "dag_id": dag_id,
                "days": days,
                "task_count": len(task_ids),
                "run_date_count": len(execution_dates),
                "cell_count": len(cells),
                "client_filtered_cutoff_date": cutoff_date,
            },
            "task_ids": task_ids,
            "execution_dates": execution_dates,
            "cells": cells,
        }

    async def get_dag_graph(
        self,
        environment_name: str,
        dag_id: str,
    ) -> Dict[str, Any]:
        """Return the task dependency graph of a DAG.

        Pulls Airflow's ``/dags/{dag_id}/tasks`` (each task carries
        ``downstream_task_ids``), then builds:

        - ``nodes``: [{id, operator, trigger_rule, retries, ...}]
        - ``edges``: [{from, to}]
        - ``mermaid``: a ``flowchart LR`` string for quick text rendering

        The graph is also wrapped in ``meta.ui`` pointing at the
        ``ui://mwaa/dag-graph`` MCP App, so hosts that support it render an
        interactive view automatically.
        """
        raw = self._invoke_airflow_api(
            environment_name, "GET", f"/dags/{dag_id}/tasks"
        )
        if "error" in raw:
            return raw

        body = raw.get("RestApiResponse") or {}
        tasks = body.get("tasks", []) or []

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []
        for t in tasks:
            tid = t.get("task_id")
            if not tid:
                continue
            nodes.append(
                {
                    "id": tid,
                    "operator": t.get("class_ref", {}).get("class_name")
                    or t.get("operator_name"),
                    "trigger_rule": t.get("trigger_rule"),
                    "retries": t.get("retries"),
                    "pool": t.get("pool"),
                    "depends_on_past": t.get("depends_on_past"),
                }
            )
            for ds in t.get("downstream_task_ids", []) or []:
                edges.append({"from": tid, "to": ds})

        mermaid = _build_mermaid(dag_id, nodes, edges)
        return {
            "summary": {
                "environment_name": environment_name,
                "dag_id": dag_id,
                "node_count": len(nodes),
                "edge_count": len(edges),
            },
            "nodes": nodes,
            "edges": edges,
            "mermaid": mermaid,
        }

    async def list_recent_failures(
        self,
        environment_name: str,
        dag_id: Optional[str] = None,
        days: int = 7,
        limit: int = 50,
        include_upstream_failed: bool = True,
    ) -> Dict[str, Any]:
        """Newest-first list of failed DAG runs (and optionally task instances).

        If ``dag_id`` is given, returns the most recent failed runs of that
        DAG. If ``dag_id`` is omitted, returns the most recent failed task
        instances across all DAGs in the environment — useful for "what
        broke overnight?" type questions.

        Both modes always sort newest-first.
        """
        cutoff = (
            (datetime.now(timezone.utc) - timedelta(days=days))
            .replace(microsecond=0)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        if dag_id:
            # DAG runs only have ``failed`` — ``upstream_failed`` is a task
            # instance state and Airflow's validator rejects it here.
            #
            # Airflow's REST API silently ignores execution_date_gte on this
            # endpoint, so we send it (in case behavior changes) AND apply a
            # client-side filter on start_date below.
            result = await self.list_dag_runs(
                environment_name=environment_name,
                dag_id=dag_id,
                limit=limit,
                state=["failed"],
                execution_date_gte=cutoff,
                order_by="-start_date",
                page=1,
                page_size=min(limit, DEFAULT_PAGE_SIZE),
            )
            if "error" in result:
                return result
            runs = result.get("dag_runs", []) or []
            filtered = [r for r in runs if (r.get("start_date") or "") >= cutoff]
            result["dag_runs"] = filtered
            summary = result.get("summary") or {}
            summary["client_filtered_cutoff"] = cutoff
            summary["client_filtered_count"] = len(filtered)
            result["summary"] = summary
            return result

        ti_states = ["failed"]
        if include_upstream_failed:
            ti_states.append("upstream_failed")
        result = await self.list_task_instances(
            environment_name=environment_name,
            start_date_gte=cutoff,
            state=ti_states,
            limit=limit,
            page=1,
            page_size=min(limit, DEFAULT_PAGE_SIZE),
        )
        if "error" in result:
            return result

        # Airflow's batch task-instances endpoint silently ignores
        # ``start_date_gte`` and defaults to oldest-first ordering. Without
        # client-side handling, the "most recent failures in the last N days"
        # contract is broken — callers got months-old failures instead of
        # the recent ones they asked for. Filter + sort here so the surface
        # behavior matches the docstring.
        tis = result.get("task_instances", []) or []
        tis = [t for t in tis if (t.get("start_date") or "") >= cutoff]
        tis.sort(key=lambda t: t.get("start_date") or "", reverse=True)
        result["task_instances"] = tis
        summary = result.get("summary") or {}
        summary["client_filtered_cutoff"] = cutoff
        summary["client_filtered_count"] = len(tis)
        summary["client_sorted"] = "start_date desc"
        result["summary"] = summary
        return result

    async def summarize_task_failure(
        self,
        environment_name: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        task_try_number: Optional[int] = None,
        context_lines: int = 4,
    ) -> Dict[str, Any]:
        """Fetch a task's log and return the lines that explain the failure.

        Runs heuristic matchers for dbt FAIL/ERROR/Runtime Error, Python
        tracebacks/exceptions, Airflow "Task failed" markers, and non-zero
        exit codes. Returns:

            {
              "headline": "...",            # one-line synopsis
              "dbt_done_stats": {...},      # if dbt ran a build
              "dbt_test_failures": [...],   # each with line_no + context
              "python_exceptions": [...],
              ...
              "resource_uri": "mwaa://logs/...",  # full log if needed
            }
        """
        log_resp = await self.get_task_logs(
            environment_name, dag_id, dag_run_id, task_id, task_try_number, full=True
        )
        if "error" in log_resp:
            return log_resp

        log_text = str(log_resp.get("log_text", ""))
        summary = summarize_log(log_text, context_lines=context_lines)

        return {
            "summary": {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "task_id": task_id,
                "task_try_number": task_try_number,
            },
            **summary.to_dict(),
            "resource_uri": encode_log_resource_uri(
                environment_name, dag_id, dag_run_id, task_id, task_try_number
            ),
        }

    async def list_task_instances(
        self,
        environment_name: str,
        dag_id: Optional[str] = None,
        dag_run_id: Optional[str] = None,
        start_date_gte: Optional[str] = None,
        start_date_lte: Optional[str] = None,
        end_date_gte: Optional[str] = None,
        end_date_lte: Optional[str] = None,
        execution_date_gte: Optional[str] = None,
        execution_date_lte: Optional[str] = None,
        state: Optional[List[str]] = None,
        pool: Optional[str] = None,
        queue: Optional[str] = None,
        duration_gte: Optional[float] = None,
        duration_lte: Optional[float] = None,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Dict[str, Any]:
        """List task instances across DAGs with flexible filtering via Airflow API.
        
        Uses the batch task instances endpoint which supports wildcards:
        - dag_id='~' means all DAGs
        - dag_run_id='~' means all DAG runs
        
        This enables time-range queries to find all tasks running in a specific window.
        """
        # Use wildcards if not specified
        dag_path = dag_id if dag_id else "~"
        run_path = dag_run_id if dag_run_id else "~"
        
        params: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        
        # Time-based filters
        if start_date_gte:
            params["start_date_gte"] = start_date_gte
        if start_date_lte:
            params["start_date_lte"] = start_date_lte
        if end_date_gte:
            params["end_date_gte"] = end_date_gte
        if end_date_lte:
            params["end_date_lte"] = end_date_lte
        if execution_date_gte:
            params["execution_date_gte"] = execution_date_gte
        if execution_date_lte:
            params["execution_date_lte"] = execution_date_lte
            
        # State and resource filters
        if state:
            params["state"] = state
        if pool:
            params["pool"] = pool
        if queue:
            params["queue"] = queue
            
        # Duration filters
        if duration_gte is not None:
            params["duration_gte"] = duration_gte
        if duration_lte is not None:
            params["duration_lte"] = duration_lte
        
        endpoint = f"/dags/{dag_path}/dagRuns/{run_path}/taskInstances"
        raw = self._invoke_airflow_api(environment_name, "GET", endpoint, params=params)
        if "error" in raw:
            return raw

        body = raw.get("RestApiResponse") or {}
        instances = body.get("task_instances", []) or []
        total = body.get("total_entries", len(instances))

        return with_resource_link(
            summary={
                "environment_name": environment_name,
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "total_entries_reported_by_airflow": total,
                "filters": {
                    "state": state,
                    "start_date_gte": start_date_gte,
                    "start_date_lte": start_date_lte,
                    "end_date_gte": end_date_gte,
                    "end_date_lte": end_date_lte,
                    "execution_date_gte": execution_date_gte,
                    "execution_date_lte": execution_date_lte,
                    "pool": pool,
                    "queue": queue,
                    "duration_gte": duration_gte,
                    "duration_lte": duration_lte,
                },
            },
            items=instances,
            page=page,
            page_size=page_size,
            resource_path=f"task_instances/{environment_name}/{dag_path}/{run_path}",
            resource_params={
                "limit": limit,
                "offset": offset,
                "state": state,
                "start_date_gte": start_date_gte,
                "start_date_lte": start_date_lte,
                "end_date_gte": end_date_gte,
                "end_date_lte": end_date_lte,
                "execution_date_gte": execution_date_gte,
                "execution_date_lte": execution_date_lte,
                "pool": pool,
                "queue": queue,
                "duration_gte": duration_gte,
                "duration_lte": duration_lte,
            },
            item_key="task_instances",
        )

    async def list_connections(
        self,
        environment_name: str,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
    ) -> Dict[str, Any]:
        """List connections via Airflow API."""
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        return self._invoke_airflow_api(
            environment_name, "GET", "/connections", params=params
        )

    async def list_variables(
        self,
        environment_name: str,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
    ) -> Dict[str, Any]:
        """List variables via Airflow API."""
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        return self._invoke_airflow_api(
            environment_name, "GET", "/variables", params=params
        )

    async def get_import_errors(
        self,
        environment_name: str,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
    ) -> Dict[str, Any]:
        """Get import errors via Airflow API."""
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        return self._invoke_airflow_api(
            environment_name, "GET", "/dags/importErrors", params=params
        )
