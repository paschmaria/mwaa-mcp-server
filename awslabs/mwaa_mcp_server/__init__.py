"""MWAA MCP Server - Model Context Protocol server for Amazon Managed Workflows for Apache Airflow."""

from .server import mcp, main
from .tools import MWAATools

__all__ = ["mcp", "main", "MWAATools"]
__version__ = "1.0.0"