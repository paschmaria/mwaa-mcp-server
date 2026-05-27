# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-05-27

### Fixed
- `_invoke_airflow_api` now surfaces the underlying Airflow HTTP status code and response body when MWAA returns `RestApiClientException` / `RestApiServerException`. Previously these came back as `{"error": "An error occurred (RestApiClientException) ... : "}` with no diagnostic information — callers couldn't tell a 404 (endpoint missing/renamed in Airflow 3.x) from a 422 (bad param) from a 5xx. Errors now also include `error_code`, `rest_api_status_code`, and `rest_api_response` fields.
- `list_recent_failures` (task-instance mode) now actually returns recent failures. Airflow's batch task-instances endpoint silently ignores `start_date_gte` and defaults to oldest-first ordering, so the previous implementation returned months-old failures in response to a `days=3` query. Now applies the date filter and newest-first sort client-side, with the cutoff exposed in the summary as `client_filtered_cutoff` / `client_filtered_count` / `client_sorted`.
- `summarize_task_failure` docstring no longer advertises framework-specific output keys that the implementation never produced. The documented `Returns:` shape now matches what `LogSummary.to_dict()` actually emits.

### Added
- 4 new tests covering the boto error-surfacing path and the client-side filter/sort in `list_recent_failures` (both task-instance and dag-id branches).

## [0.1.0] - 2026-03-06

### Changed
- Restructured project to conform to awslabs/mcp repository contribution standards
- Replaced loguru with Python standard library logging
- Updated fastmcp dependency to >=3.0.0
- Replaced flake8 with ruff for linting
- Switched from requirements.txt to pyproject.toml dependency groups
- Updated entry point to `awslabs.mwaa_mcp_server.main:main`
- Aligned code style to 100-char line length

### Added
- `.python-version` file (3.10)
- `pyrightconfig.json` for type checking
- `server.json` for hosted MCP server metadata
- `DEVELOPMENT.md` with contributor setup instructions
- Expanded test coverage for Airflow API tools, readonly mode, and token creation

### Removed
- `requirements.txt` and `requirements-dev.txt` (replaced by pyproject.toml)
- `loguru` and `httpx` dependencies
- Dead `TestAirflowClient` tests referencing removed `airflow_client.py`

## [1.0.1] - 2025-08-06

### Fixed
- Fixed authentication issue with Airflow REST API by using CLI token directly as Bearer token
- Fixed response handling to return actual API data instead of just success messages
- Added debug information for 401 authentication errors to help with troubleshooting

### Removed
- Removed unused `AirflowClient` class as the implementation now uses AWS SDK's `invoke_rest_api` method directly

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
