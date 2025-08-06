"""MWAA MCP Server Tools - Implementation of MWAA operations."""

import os
from typing import Any, Dict, List, Optional

import boto3
from loguru import logger
from botocore.exceptions import ClientError, BotoCoreError


class MWAATools:
    """Tools for interacting with Amazon MWAA."""

    def __init__(self):
        """Initialize MWAA tools with AWS clients."""
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.readonly = os.getenv("MWAA_MCP_READONLY", "true").lower() == "true"

        self.mwaa_client = boto3.client("mwaa", region_name=self.region)

        logger.info(f"Initialized MWAA tools for region: {self.region}")
        if self.readonly:
            logger.info("Running in read-only mode")

    def _check_readonly(self, operation: str) -> None:
        """Check if operation is allowed in read-only mode."""
        if self.readonly:
            raise PermissionError(
                f"Operation '{operation}' not allowed in read-only mode"
            )

    def _invoke_airflow_api(
        self, environment_name: str, method: str, path: str, **kwargs
    ) -> Dict[str, Any]:
        """Invoke Airflow REST API using MWAA client."""
        try:
            # Prepare the request parameters
            params = {
                "Name": environment_name,
                "Method": method.upper(),
                "Path": path,
            }

            # Add query parameters if provided
            if "params" in kwargs:
                query_string = "&".join(
                    [f"{k}={v}" for k, v in kwargs["params"].items()]
                )
                if query_string:
                    params["Path"] = f"{path}?{query_string}"

            # Add JSON body if provided
            if "json_data" in kwargs:
                import json

                params["Body"] = json.dumps(kwargs["json_data"])

            response = self.mwaa_client.invoke_rest_api(**params)

            return response

        except Exception as e:
            logger.error(f"Error invoking Airflow API {method} {path}: {e}")
            return {"error": str(e)}

    # Environment Management Methods
    async def list_environments(
        self, max_results: Optional[int] = None
    ) -> Dict[str, Any]:
        """List MWAA environments."""
        try:
            kwargs = {}
            if max_results:
                kwargs["MaxResults"] = min(max_results, 25)

            response = self.mwaa_client.list_environments(**kwargs)

            # Get details for each environment
            environments = []
            for env_name in response.get("Environments", []):
                try:
                    env_details = await self.get_environment(env_name)
                    environments.append(
                        {
                            "Name": env_name,
                            "Status": env_details.get("Environment", {}).get("Status"),
                            "Arn": env_details.get("Environment", {}).get("Arn"),
                            "CreatedAt": env_details.get("Environment", {}).get(
                                "CreatedAt"
                            ),
                        }
                    )
                except Exception as e:
                    logger.error(
                        f"Error getting details for environment {env_name}: {e}"
                    )
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
            logger.error(f"Error listing environments: {e}")
            return {"error": str(e)}

    async def get_environment(self, name: str) -> Dict[str, Any]:
        """Get environment details."""
        try:
            response = self.mwaa_client.get_environment(Name=name)

            # Convert datetime objects to strings
            env = response.get("Environment", {})
            if "CreatedAt" in env:
                env["CreatedAt"] = env["CreatedAt"].isoformat()
            if "LastUpdate" in env and "CreatedAt" in env["LastUpdate"]:
                env["LastUpdate"]["CreatedAt"] = env["LastUpdate"][
                    "CreatedAt"
                ].isoformat()

            return {"Environment": env}

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Error getting environment {name}: {e}")
            return {"error": str(e)}

    async def create_environment(self, **kwargs) -> Dict[str, Any]:
        """Create a new MWAA environment."""
        self._check_readonly("create_environment")

        try:
            # Filter out None values
            params = {k: v for k, v in kwargs.items() if v is not None}

            # Convert parameter names to PascalCase for boto3
            boto_params = {}
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
            logger.error(f"Error creating environment: {e}")
            return {"error": str(e)}

    async def update_environment(self, **kwargs) -> Dict[str, Any]:
        """Update an existing MWAA environment."""
        self._check_readonly("update_environment")

        try:
            # Filter out None values
            params = {k: v for k, v in kwargs.items() if v is not None}

            # Convert parameter names to PascalCase for boto3
            boto_params = {}
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
            logger.error(f"Error updating environment: {e}")
            return {"error": str(e)}

    async def delete_environment(self, name: str) -> Dict[str, Any]:
        """Delete an existing MWAA environment."""
        self._check_readonly("delete_environment")

        try:
            self.mwaa_client.delete_environment(Name=name)
            return {"message": f"Environment {name} deleted successfully"}

        except (ClientError, BotoCoreError) as e:
            logger.error(f"Error deleting environment {name}: {e}")
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
            logger.error(f"Error creating CLI token for {name}: {e}")
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
            logger.error(f"Error creating web login token for {name}: {e}")
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
        params = {
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

    async def get_dag_source(
        self, environment_name: str, dag_id: str
    ) -> Dict[str, Any]:
        """Get DAG source code via Airflow API."""
        return self._invoke_airflow_api(
            environment_name, "GET", f"/dagSources/{dag_id}"
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

        data = {}

        if dag_run_id:
            data["dag_run_id"] = dag_run_id
        else:
            # Generate a unique run ID
            from datetime import datetime

            data["dag_run_id"] = f"manual__{datetime.utcnow().isoformat()}"

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
    ) -> Dict[str, Any]:
        """List DAG runs via Airflow API."""
        params = {"limit": limit}

        if state:
            params["state"] = state
        if execution_date_gte:
            params["execution_date_gte"] = execution_date_gte
        if execution_date_lte:
            params["execution_date_lte"] = execution_date_lte

        return self._invoke_airflow_api(
            environment_name, "GET", f"/dags/{dag_id}/dagRuns", params=params
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
    ) -> Dict[str, Any]:
        """Get task logs via Airflow API."""
        endpoint = f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs"
        if task_try_number is not None:
            endpoint += f"/{task_try_number}"
        return self._invoke_airflow_api(environment_name, "GET", endpoint)

    async def list_connections(
        self,
        environment_name: str,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
    ) -> Dict[str, Any]:
        """List connections via Airflow API."""
        params = {"limit": limit, "offset": offset}
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
        params = {"limit": limit, "offset": offset}
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
        params = {"limit": limit, "offset": offset}
        return self._invoke_airflow_api(
            environment_name, "GET", "/importErrors", params=params
        )
