# DataHub Audit Log Downloader

This script downloads audit logs from DataHub using the [Audit Events Search API](https://docs.datahub.com/docs/actions/events/audit-events-search-guide).

## Features

- Download audit logs with flexible date ranges
- Filter by event types, entity types, aspect types, and actors
- Multiple output formats: JSON, JSONL, CSV
- Paginated retrieval with configurable page size
- Optional inclusion of raw event data

## Installation

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

The script uses the DataHub Python client's default configuration. Set up your connection using environment variables:

```bash
export DATAHUB_GMS_URL=https://your-instance.acryl.io/gms
export DATAHUB_GMS_TOKEN=your-token-here
```

Or configure via `~/.datahubenv`:

```
DATAHUB_GMS_URL=https://your-instance.acryl.io/gms
DATAHUB_GMS_TOKEN=your-token-here
```

## Usage

### Basic Examples

Download last 24 hours of logs (default):
```bash
python script.py
```

Download logs for specific date range:
```bash
python script.py --start-date "2025-01-01 00:00:00" --end-date "2025-01-02 00:00:00"
```

Download logs filtered by event type:
```bash
python script.py --event-types "LogInEvent,FailedLogInEvent"
```

Download logs for specific user:
```bash
python script.py --actor-urns "urn:li:corpuser:john.doe"
```

Export to CSV:
```bash
python script.py --format csv --output audit_logs.csv
```

Export to JSONL (one event per line):
```bash
python script.py --format jsonl --output audit_logs.jsonl
```

### Command Line Options

- `--output`, `-o`: Output file path (default: audit_logs.json)
- `--format`: Output format - json, jsonl, or csv (default: json)
- `--start-date`: Start date in "YYYY-MM-DD HH:MM:SS" format (default: 24 hours ago)
- `--end-date`: End date in "YYYY-MM-DD HH:MM:SS" format (default: now)
- `--event-types`: Comma-separated list of event types to filter
- `--entity-types`: Comma-separated list of entity types to filter
- `--aspect-types`: Comma-separated list of aspect types to filter
- `--actor-urns`: Comma-separated list of actor URNs to filter
- `--page-size`: Number of events per page (default: 100)
- `--include-raw/--no-include-raw`: Include raw event data (default: true)

## Output Formats

### JSON
Standard JSON array format with pretty printing:
```json
[
  {
    "eventType": "LogInEvent",
    "timestamp": 1765812228069,
    "actorUrn": "urn:li:corpuser:admin",
    ...
  },
  ...
]
```

### JSONL
One JSON object per line (newline-delimited JSON):
```json
{"eventType": "LogInEvent", "timestamp": 1765812228069, ...}
{"eventType": "UpdateIngestionSourceEvent", "timestamp": 1765801335566, ...}
```

### CSV
Flattened format with key fields:
```csv
eventType,timestamp,actorUrn,sourceIP,eventSource,userAgent
LogInEvent,1765812228069,urn:li:corpuser:admin,,,
```

## Analyzing Logs

The JSON output can be processed with `jq`:

```bash
# Extract all event types
jq -r '.[].eventType' audit_logs.json | sort -u

# Count events by type
jq -r '.[].eventType' audit_logs.json | sort | uniq -c | sort -rn

# Filter events by specific user
jq '.[] | select(.actorUrn == "urn:li:corpuser:admin")' audit_logs.json

# Extract login events
jq '.[] | select(.eventType == "LogInEvent")' audit_logs.json
```

## Common Event Types

- `LogInEvent` - User login
- `FailedLogInEvent` - Failed login attempt
- `CreateAccessTokenEvent` - Access token creation
- `UpdateIngestionSourceEvent` - Ingestion source configuration change
- `UpdateUserEvent` - User profile update
-  And more, see https://docs.datahub.com/docs/actions/events/audit-events-search-guide#createupdatedelete-event-types

## Requirements

- Python 3.7+
- acryl-datahub
- click
- rich

See [requirements.txt](../requirements.txt) for full dependency list.
