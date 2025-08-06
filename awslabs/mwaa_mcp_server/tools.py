"""MWAA MCP Server Tools - Implementation of MWAA operations."""

import os
from typing import Any, Dict, List, Optional

import boto3
import httpx
from loguru import logger
from botocore.exceptions import ClientError, BotoCoreError

from .airflow_client import AirflowClient


class MWAATools:
    """Tools for interacting with Amazon MWAA."""

    def __init__(self):
        """Initialize MWAA tools with AWS clients."""
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.profile = os.getenv("AWS_PROFILE")
        self.readonly = os.getenv("MWAA_MCP_READONLY", "false").lower() == "true"
        
        # Initialize boto3 session
        session_kwargs = {"region_name": self.region}
        if self.profile:
            session_kwargs["profile_name"] = self.profile
            
        self.session = boto3.Session(**session_kwargs)
        self.mwaa_client = self.session.client("mwaa")
        
        # Cache for Airflow clients per environment
        self._airflow_clients: Dict[str, AirflowClient] = {}
        
        logger.info(f"Initialized MWAA tools for region: {self.region}")
        if self.readonly:
            logger.info("Running in read-only mode")

    def _check_readonly(self, operation: str) -> None:
        """Check if operation is allowed in read-only mode."""
        if self.readonly:
            raise PermissionError(f"Operation '{operation}' not allowed in read-only mode")

    async def _get_airflow_client(self, environment_name: str) -> AirflowClient:
        """Get or create an Airflow client for the environment."""
        if environment_name not in self._airflow_clients:
            # Get CLI token for API access
            token_response = self.mwaa_client.create_cli_token(Name=environment_name)
            
            self._airflow_clients[environment_name] = AirflowClient(
                webserver_hostname=token_response["WebServerHostname"],
                cli_token=token_response["CliToken"],
            )
        
        return self._airflow_clients[environment_name]

    # Environment Management Methods
    async def list_environments(self, max_results: Optional[int] = None) -> Dict[str, Any]:
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
                    environments.append({
                        "Name": env_name,
                        "Status": env_details.get("Environment", {}).get("Status"),
                        "Arn": env_details.get("Environment", {}).get("Arn"),
                        "CreatedAt": env_details.get("Environment", {}).get("CreatedAt"),
                    })
                except Exception as e:
                    logger.error(f"Error getting details for environment {env_name}: {e}")
                    environments.append({
                        "Name": env_name,
                        "Status": "ERROR",
                        "Error": str(e),
                    })
            
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
                env["LastUpdate"]["CreatedAt"] = env["LastUpdate"]["CreatedAt"].isoformat()
                
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
        """Update an MWAA environment."""
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
        """Delete an MWAA environment."""
        self._check_readonly("delete_environment")
        
        try:
            self.mwaa_client.delete_environment(Name=name)
            return {"message": f"Environment '{name}' deletion initiated"}
            
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Error deleting environment {name}: {e}")
            return {"error": str(e)}

    async def create_cli_token(self, name: str) -> Dict[str, Any]:
        """Create a CLI token."""
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
        """Create a web login token."""
        try:
            response = self.mwaa_client.create_web_login_token(Name=name)
            return {
                "WebToken": response["WebToken"],
                "WebServerHostname": response["WebServerHostname"],
                "IamIdentity": response.get("IamIdentity"),
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
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.list_dags(limit, offset, tags, dag_id_pattern, only_active)
        except Exception as e:
            logger.error(f"Error listing DAGs: {e}")
            return {"error": str(e)}

    async def get_dag(self, environment_name: str, dag_id: str) -> Dict[str, Any]:
        """Get DAG details via Airflow API."""
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.get_dag(dag_id)
        except Exception as e:
            logger.error(f"Error getting DAG {dag_id}: {e}")
            return {"error": str(e)}

    async def get_dag_source(self, environment_name: str, dag_id: str) -> Dict[str, Any]:
        """Get DAG source code via Airflow API."""
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.get_dag_source(dag_id)
        except Exception as e:
            logger.error(f"Error getting DAG source for {dag_id}: {e}")
            return {"error": str(e)}

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
        
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.trigger_dag_run(dag_id, dag_run_id, conf, note)
        except Exception as e:
            logger.error(f"Error triggering DAG {dag_id}: {e}")
            return {"error": str(e)}

    async def get_dag_run(
        self, environment_name: str, dag_id: str, dag_run_id: str
    ) -> Dict[str, Any]:
        """Get DAG run details via Airflow API."""
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.get_dag_run(dag_id, dag_run_id)
        except Exception as e:
            logger.error(f"Error getting DAG run {dag_run_id}: {e}")
            return {"error": str(e)}

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
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.list_dag_runs(
                dag_id, limit, state, execution_date_gte, execution_date_lte
            )
        except Exception as e:
            logger.error(f"Error listing DAG runs for {dag_id}: {e}")
            return {"error": str(e)}

    async def get_task_instance(
        self, environment_name: str, dag_id: str, dag_run_id: str, task_id: str
    ) -> Dict[str, Any]:
        """Get task instance details via Airflow API."""
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.get_task_instance(dag_id, dag_run_id, task_id)
        except Exception as e:
            logger.error(f"Error getting task instance {task_id}: {e}")
            return {"error": str(e)}

    async def get_task_logs(
        self,
        environment_name: str,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        task_try_number: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Get task logs via Airflow API."""
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.get_task_logs(dag_id, dag_run_id, task_id, task_try_number)
        except Exception as e:
            logger.error(f"Error getting task logs for {task_id}: {e}")
            return {"error": str(e)}

    async def list_connections(
        self, environment_name: str, limit: Optional[int] = 100, offset: Optional[int] = 0
    ) -> Dict[str, Any]:
        """List Airflow connections via API."""
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.list_connections(limit, offset)
        except Exception as e:
            logger.error(f"Error listing connections: {e}")
            return {"error": str(e)}

    async def list_variables(
        self, environment_name: str, limit: Optional[int] = 100, offset: Optional[int] = 0
    ) -> Dict[str, Any]:
        """List Airflow variables via API."""
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.list_variables(limit, offset)
        except Exception as e:
            logger.error(f"Error listing variables: {e}")
            return {"error": str(e)}

    async def get_import_errors(
        self, environment_name: str, limit: Optional[int] = 100, offset: Optional[int] = 0
    ) -> Dict[str, Any]:
        """Get DAG import errors via API."""
        try:
            client = await self._get_airflow_client(environment_name)
            return await client.get_import_errors(limit, offset)
        except Exception as e:
            logger.error(f"Error getting import errors: {e}")
            return {"error": str(e)}
