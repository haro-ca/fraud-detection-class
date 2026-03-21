# Phase 9: MCP Server

## Issue Addressed
- #7: MCP server for application status and fraud detection

## What Was Built

Model Context Protocol server exposing fraud detection tools:

```bash
uv run python mcp_server/main.py
```

## Available Tools

### 1. `get_application_status`
Get status and fraud results for a specific application.

```json
{
  "name": "get_application_status",
  "arguments": {
    "application_id": 123
  }
}
```

### 2. `get_all_applications`
List credit applications with optional status filter.

```json
{
  "name": "get_all_applications",
  "arguments": {
    "status": "pending",
    "limit": 20
  }
}
```

### 3. `get_fraud_summary`
Get summary of fraud detection results.

```json
{
  "name": "get_fraud_summary",
  "arguments": {}
}
```

Returns:
```json
{
  "applications": {
    "total": 24,
    "approved": 10,
    "rejected": 14,
    "pending": 0
  },
  "fraud_flags": 29,
  "rules_triggered": {
    "velocity_check": 13,
    "geo_anomaly": 14,
    "income_ratio": 2
  }
}
```

### 4. `run_fraud_check`
Run fraud detection for a specific application.

```json
{
  "name": "run_fraud_check",
  "arguments": {
    "application_id": 123
  }
}
```

## Architecture

```
AI Assistant  ──MCP──>  MCP Server  ──Django ORM──>  Neon DB
                        │
                        └── fraud_detection_server.py
```

## Files Created
- `mcp_server/server.py` (new)
- `mcp_server/main.py` (new)

## Dependencies Added
- `mcp` - MCP SDK
- `uvicorn` - ASGI server
