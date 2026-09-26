# TimeCamp Data Pipeline

A data pipeline to extract TimeCamp datasets from REST API (time entries, computer activities, users, tasks, application names) and load them into various destinations (Google Big Query, S3, CSV, JSONL, Parquet, MySQL, Postgres, DuckDB, SQLite - any DLT destination).

## Run

```bash
uv run --with-requirements requirements.txt dlt_fetch_timecamp.py \
   --from 2026-01-01 --to 2026-09-26 \
   --datasets entries,tasks,users,computer_activities,application_names \
   --format jsonl \
   --custom-fields \
   --output ./output
```

Use `--destination` with a dlt destination name and install its required dlt extra.
For Google BigQuery setup, see [Export to BigQuery](docs/export-to-bigquery.md).

## Available Datasets

| Dataset | Description |
|---------|-------------|
| `entries` | Time entries with project/task details |
| `tasks` | Projects & tasks hierarchy with breadcrumb paths and details |
| `users` | User details with group information |
| `computer_activities` | Desktop app tracking data |
| `application_names` | Application lookup table with names and categories |

## Documentation

- [Export to BigQuery](docs/export-to-bigquery.md) - load datasets and custom fields into Google BigQuery
- [Fetch Project Data to S3](docs/fetch-project-data-to-s3.md) - write files to S3 and query them with DuckDB
- [Sample Reports](docs/SAMPLE-REPORTS.md) - estimated vs actual time report with DuckDB
- [Sample Project Cumulative vs Budgeted Report](docs/SAMPLE-PROJECT-BUDGET-REPORT.md) - project budget report with DuckDB

## License

MIT
