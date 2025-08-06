# MWAA MCP Server

[![PyPI](https://img.shields.io/pypi/v/awslabs.mwaa-mcp-server.svg)](https://pypi.org/project/awslabs.mwaa-mcp-server/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Model Context Protocol (MCP) server for Amazon Managed Workflows for Apache Airflow (MWAA).

This MCP server provides comprehensive tools for managing MWAA environments and interacting with Apache Airflow through a unified interface. It enables AI assistants to help with workflow orchestration, DAG management, and operational tasks.

## Features

### MWAA Environment Management
- **List and describe environments** - View all MWAA environments and their configurations
- **Create and update environments** - Deploy new environments or modify existing ones
- **Delete environments** - Clean up unused environments
- **Generate access tokens** - Create CLI and web UI access tokens

### Airflow Operations
- **DAG Management** - List, view, and trigger DAGs
- **DAG Runs** - Monitor and manage workflow executions
- **Task Instances** - Track individual task status and logs
- **Connections & Variables** - View Airflow connections and variables
- **Import Errors** - Diagnose DAG parsing issues

### Expert Guidance
- **Best Practices** - Get MWAA and Airflow best practices
- **DAG Design** - Expert guidance on workflow design patterns

## Installation

### Prerequisites

1. **AWS Credentials**: Configure AWS credentials with appropriate permissions for MWAA
2. **Python**: Python 3.10 or higher
3. **uv**: Install uv for package management (recommended)

### Quick Start

#### Using uvx (Recommended)

```bash
# Run directly without installation
uvx awslabs.mwaa-mcp-server

# Or install globally
uv tool install awslabs.mwaa-mcp-server
```

#### Using pip

```bash
pip install awslabs.mwaa-mcp-server
```

#### Using Docker

```bash
docker run -it --rm \
  -e AWS_PROFILE=default \
  -e AWS_REGION=us-east-1 \
  -v ~/.aws:/root/.aws:ro \
  awslabs/mwaa-mcp-server
```

## Configuration

### Environment Variables

- `AWS_PROFILE` - AWS credential profile to use (default: uses AWS credential chain)
- `AWS_REGION` - AWS region for MWAA operations (default: us-east-1)
- `MWAA_MCP_READONLY` - Set to "true" for read-only mode
- `FASTMCP_LOG_LEVEL` - Logging level: ERROR, WARNING, INFO, DEBUG (default: ERROR)

### MCP Client Configuration

Add to your MCP client configuration file:

#### Claude Desktop

`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS)
`%APPDATA%\Claude\claude_desktop_config.json` (Windows)

```json
{
  "mcpServers": {
    "mwaa": {
      "command": "uvx",
      "args": ["awslabs.mwaa-mcp-server"],
      "env": {
        "AWS_PROFILE": "your-profile",
        "AWS_REGION": "us-east-1",
        "MWAA_MCP_READONLY": "false",
        "FASTMCP_LOG_LEVEL": "ERROR"
      }
    }
  }
}
```

#### Other MCP Clients

Refer to your MCP client's documentation for configuration details.

## Usage Examples

### Environment Management

```
"List all MWAA environments in my account"
"Show me details about the 'production-airflow' environment"
"Create a new MWAA environment called 'dev-airflow' with 2 schedulers"
"Update the production environment to use Airflow 2.7.2"
```

### DAG Operations

```
"List all DAGs in the production environment"
"Show me the source code for the 'etl_pipeline' DAG"
"Trigger the 'daily_report' DAG with config {'date': '2024-01-01'}"
"Check the status of the latest run for 'data_processing' DAG"
```

### Monitoring and Troubleshooting

```
"Show me failed DAG runs from the last 24 hours"
"Get logs for the 'extract_data' task that failed"
"List all import errors in the development environment"
"Show me all Airflow connections configured in production"
```

### Best Practices

```
"What are the best practices for MWAA environment sizing?"
"How should I design a DAG for parallel data processing?"
"Give me guidance on handling errors in Airflow tasks"
```

## Required AWS Permissions

The IAM user or role needs the following permissions:

### MWAA Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "airflow:CreateCliToken",
        "airflow:CreateWebLoginToken",
        "airflow:GetEnvironment",
        "airflow:ListEnvironments"
      ],
      "Resource": "*"
    }
  ]
}
```

### Additional Permissions for Write Operations
```json
{
  "Effect": "Allow",
  "Action": [
    "airflow:CreateEnvironment",
    "airflow:UpdateEnvironment",
    "airflow:DeleteEnvironment"
  ],
  "Resource": "*"
}
```

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/awslabs/mcp.git
cd mcp/src/mwaa-mcp-server

# Create virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode
uv sync --dev
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=awslabs.mwaa_mcp_server

# Run specific test
pytest tests/test_tools.py::test_list_environments
```

### Building Docker Image

```bash
# Build image
docker build -t awslabs/mwaa-mcp-server .

# Run container
docker run -it --rm \
  -e AWS_PROFILE=default \
  -v ~/.aws:/root/.aws:ro \
  awslabs/mwaa-mcp-server
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify AWS credentials are configured correctly
   - Check IAM permissions for MWAA operations
   - Ensure the correct AWS region is specified

2. **Connection Timeouts**
   - Check VPC and security group configurations
   - Verify MWAA environment is in AVAILABLE state
   - Ensure your network can reach MWAA endpoints

3. **Import Errors in DAGs**
   - Use the `get_import_errors` tool to diagnose
   - Check CloudWatch logs for detailed error messages
   - Verify all dependencies are in requirements.txt

### Debug Mode

Enable debug logging for troubleshooting:

```bash
export FASTMCP_LOG_LEVEL=DEBUG
uvx awslabs.mwaa-mcp-server
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- 📚 [Documentation](https://awslabs.github.io/mcp/servers/mwaa-mcp-server/)
- 🐛 [Issue Tracker](https://github.com/awslabs/mcp/issues)
- 💬 [Discussions](https://github.com/awslabs/mcp/discussions)

## Acknowledgments

This MCP server is part of the [AWS Labs MCP Servers](https://github.com/awslabs/mcp) project, bringing AWS best practices to AI-assisted development.