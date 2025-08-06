"""Airflow REST API client for MWAA."""

import json
import base64
from typing import Any, Dict, List, Optional
from datetime import datetime
from urllib.parse import urlencode

import httpx
from loguru import logger


class AirflowClient:
    """Client for interacting with Airflow REST API in MWAA."""

    def __init__(self, webserver_hostname: str, cli_token: str):
        """Initialize Airflow client.
        
        Args:
            webserver_hostname: MWAA webserver hostname
            cli_token: CLI token for authentication
        """
        self.base_url = f"https://{webserver_hostname}/api/v1"
        self.cli_token = cli_token
        self.session_token = self._get_session_token()
        
        # Create HTTP client with auth headers
        self.client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {self.session_token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    def _get_session_token(self) -> str:
        """Convert CLI token to session token."""
        # The CLI token is base64 encoded and contains the actual bearer token
        decoded = base64.b64decode(self.cli_token).decode("utf-8")
        # Extract the token from the decoded string
        # Format is typically: "airflow webserver <token>"
        parts = decoded.split()
        if len(parts) >= 3 and parts[0] == "airflow" and parts[1] == "webserver":
            return parts[2]
        return decoded

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make an authenticated request to Airflow API."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = await self.client.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
            )
            
            response.raise_for_status()
            
            if response.content:
                return response.json()
            return {"message": "Success"}
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
            return {
                "error": f"HTTP {e.response.status_code}",
                "message": e.response.text,
            }
        except Exception as e:
            logger.error(f"Request error: {e}")
            return {"error": str(e)}

    # DAG Management
    async def list_dags(
        self,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        tags: Optional[List[str]] = None,
        dag_id_pattern: Optional[str] = None,
        only_active: Optional[bool] = True,
    ) -> Dict[str, Any]:
        """List all DAGs."""
        params = {
            "limit": limit,
            "offset": offset,
            "only_active": only_active,
        }
        
        if tags:
            params["tags"] = ",".join(tags)
        if dag_id_pattern:
            params["dag_id_pattern"] = dag_id_pattern
            
        return await self._request("GET", "/dags", params=params)

    async def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """Get DAG details."""
        return await self._request("GET", f"/dags/{dag_id}")

    async def get_dag_source(self, dag_id: str) -> Dict[str, Any]:
        """Get DAG source code."""
        result = await self._request("GET", f"/dagSources/{dag_id}")
        if "content" in result:
            # Decode base64 content
            result["content"] = base64.b64decode(result["content"]).decode("utf-8")
        return result

    # DAG Runs
    async def trigger_dag_run(
        self,
        dag_id: str,
        dag_run_id: Optional[str] = None,
        conf: Optional[Dict[str, Any]] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Trigger a DAG run."""
        data = {}
        
        if dag_run_id:
            data["dag_run_id"] = dag_run_id
        else:
            # Generate a unique run ID
            data["dag_run_id"] = f"manual__{datetime.utcnow().isoformat()}"
            
        if conf:
            data["conf"] = conf
        if note:
            data["note"] = note
            
        return await self._request("POST", f"/dags/{dag_id}/dagRuns", json_data=data)

    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Get DAG run details."""
        return await self._request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}")

    async def list_dag_runs(
        self,
        dag_id: str,
        limit: Optional[int] = 100,
        state: Optional[List[str]] = None,
        execution_date_gte: Optional[str] = None,
        execution_date_lte: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List DAG runs."""
        params = {"limit": limit}
        
        if state:
            params["state"] = state
        if execution_date_gte:
            params["execution_date_gte"] = execution_date_gte
        if execution_date_lte:
            params["execution_date_lte"] = execution_date_lte
            
        return await self._request("GET", f"/dags/{dag_id}/dagRuns", params=params)

    # Task Instances
    async def get_task_instance(
        self, dag_id: str, dag_run_id: str, task_id: str
    ) -> Dict[str, Any]:
        """Get task instance details."""
        return await self._request(
            "GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        )

    async def get_task_logs(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        task_try_number: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Get task logs."""
        endpoint = f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs"
        
        params = {}
        if task_try_number is not None:
            params["try_number"] = task_try_number
            
        return await self._request("GET", endpoint, params=params)

    # Connections and Variables
    async def list_connections(
        self, limit: Optional[int] = 100, offset: Optional[int] = 0
    ) -> Dict[str, Any]:
        """List Airflow connections."""
        params = {"limit": limit, "offset": offset}
        return await self._request("GET", "/connections", params=params)

    async def list_variables(
        self, limit: Optional[int] = 100, offset: Optional[int] = 0
    ) -> Dict[str, Any]:
        """List Airflow variables."""
        params = {"limit": limit, "offset": offset}
        return await self._request("GET", "/variables", params=params)

    # Import Errors
    async def get_import_errors(
        self, limit: Optional[int] = 100, offset: Optional[int] = 0
    ) -> Dict[str, Any]:
        """Get DAG import errors."""
        params = {"limit": limit, "offset": offset}
        return await self._request("GET", "/importErrors", params=params)

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()