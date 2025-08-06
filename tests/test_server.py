"""Tests for MWAA MCP Server."""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from awslabs.mwaa_mcp_server import MWAATools


@pytest.fixture
def mock_boto_client():
    """Mock boto3 MWAA client."""
    with patch("boto3.Session") as mock_session:
        mock_client = Mock()
        mock_session.return_value.client.return_value = mock_client
        yield mock_client


@pytest.fixture
def mwaa_tools(mock_boto_client):
    """Create MWAATools instance with mocked client."""
    return MWAATools()


class TestMWAATools:
    """Test MWAA tools functionality."""

    @pytest.mark.asyncio
    async def test_list_environments(self, mwaa_tools, mock_boto_client):
        """Test listing MWAA environments."""
        # Mock response
        mock_boto_client.list_environments.return_value = {
            "Environments": ["test-env-1", "test-env-2"]
        }
        mock_boto_client.get_environment.return_value = {
            "Environment": {
                "Name": "test-env-1",
                "Status": "AVAILABLE",
                "Arn": "arn:aws:airflow:us-east-1:123456789012:environment/test-env-1",
                "CreatedAt": "2024-01-01T00:00:00Z"
            }
        }

        # Call method
        result = await mwaa_tools.list_environments()

        # Assertions
        assert "Environments" in result
        assert len(result["Environments"]) == 2
        mock_boto_client.list_environments.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_environment(self, mwaa_tools, mock_boto_client):
        """Test getting environment details."""
        # Mock response
        mock_env = {
            "Name": "test-env",
            "Status": "AVAILABLE",
            "Arn": "arn:aws:airflow:us-east-1:123456789012:environment/test-env",
            "WebserverUrl": "https://test.airflow.region.amazonaws.com",
            "ExecutionRoleArn": "arn:aws:iam::123456789012:role/airflow-execution-role",
            "ServiceRoleArn": "arn:aws:iam::123456789012:role/airflow-service-role",
            "NetworkConfiguration": {
                "SubnetIds": ["subnet-123", "subnet-456"],
                "SecurityGroupIds": ["sg-123"]
            }
        }
        mock_boto_client.get_environment.return_value = {"Environment": mock_env}

        # Call method
        result = await mwaa_tools.get_environment("test-env")

        # Assertions
        assert "Environment" in result
        assert result["Environment"]["Name"] == "test-env"
        assert result["Environment"]["Status"] == "AVAILABLE"
        mock_boto_client.get_environment.assert_called_once_with(Name="test-env")

    @pytest.mark.asyncio
    async def test_readonly_mode(self, mock_boto_client):
        """Test read-only mode restrictions."""
        with patch.dict("os.environ", {"MWAA_MCP_READONLY": "true"}):
            tools = MWAATools()
            
            # Should raise PermissionError for write operations
            with pytest.raises(PermissionError, match="not allowed in read-only mode"):
                await tools.create_environment(
                    name="test",
                    dag_s3_path="s3://bucket/dags",
                    execution_role_arn="arn:aws:iam::123456789012:role/test",
                    network_configuration={},
                    source_bucket_arn="arn:aws:s3:::bucket"
                )

    @pytest.mark.asyncio
    async def test_create_cli_token(self, mwaa_tools, mock_boto_client):
        """Test creating CLI token."""
        # Mock response
        mock_boto_client.create_cli_token.return_value = {
            "CliToken": "test-token-123",
            "WebServerHostname": "test.airflow.region.amazonaws.com"
        }

        # Call method
        result = await mwaa_tools.create_cli_token("test-env")

        # Assertions
        assert "CliToken" in result
        assert "WebServerHostname" in result
        assert result["CliToken"] == "test-token-123"
        mock_boto_client.create_cli_token.assert_called_once_with(Name="test-env")

    @pytest.mark.asyncio
    async def test_error_handling(self, mwaa_tools, mock_boto_client):
        """Test error handling for AWS API errors."""
        from botocore.exceptions import ClientError
        
        # Mock error
        mock_boto_client.get_environment.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Environment not found"}},
            "GetEnvironment"
        )

        # Call method
        result = await mwaa_tools.get_environment("non-existent")

        # Assertions
        assert "error" in result
        assert "ResourceNotFoundException" in result["error"]


class TestAirflowClient:
    """Test Airflow REST API client functionality."""

    @pytest.mark.asyncio
    async def test_list_dags(self):
        """Test listing DAGs via Airflow API."""
        from awslabs.mwaa_mcp_server.airflow_client import AirflowClient
        
        with patch("httpx.AsyncClient") as mock_client:
            # Setup mock
            mock_response = Mock()
            mock_response.json.return_value = {
                "dags": [
                    {"dag_id": "example_dag", "is_active": True},
                    {"dag_id": "tutorial_dag", "is_active": True}
                ],
                "total_entries": 2
            }
            mock_response.content = True
            mock_response.raise_for_status = Mock()
            
            mock_client.return_value.request = AsyncMock(return_value=mock_response)
            
            # Create client
            client = AirflowClient("test.airflow.amazonaws.com", "test-token")
            client.client = mock_client.return_value
            
            # Call method
            result = await client.list_dags()
            
            # Assertions
            assert "dags" in result
            assert len(result["dags"]) == 2
            assert result["dags"][0]["dag_id"] == "example_dag"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])