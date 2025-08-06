# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2025-08-06

### Fixed
- Fixed authentication issue with Airflow REST API by using CLI token directly as Bearer token
- Fixed response handling to return actual API data instead of just success messages
- Added debug information for 401 authentication errors to help with troubleshooting

## [1.0.0] - 2025-01-31

### Added
- Initial release of MWAA MCP Server
- MWAA environment management tools:
  - List, get, create, update, and delete environments
  - Create CLI and web login tokens
- Airflow REST API integration:
  - DAG management (list, get, trigger)
  - DAG run monitoring
  - Task instance tracking and logs
  - Connections and variables listing
  - Import error diagnostics
- Expert guidance tools:
  - MWAA/Airflow best practices
  - DAG design patterns and optimization
- Docker support with health checks
- Comprehensive documentation and examples
- Read-only mode for safe operations
- Support for multiple AWS profiles and regions

### Security
- IAM-based authentication
- Optional read-only mode
- Secure token handling for Airflow API access

### Documentation
- Comprehensive README with usage examples
- Detailed API documentation for all tools
- Best practices and troubleshooting guide
