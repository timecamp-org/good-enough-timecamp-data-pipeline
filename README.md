# TimeCamp Data Pipeline

A data pipeline to extract TimeCamp datasets from REST API (time entries, computer activities, users, tasks, application names) and load them into various destinations (Google Big Query, S3, CSV, JSONL, Parquet, MySQL, Postgres, DuckDB, SQLite - any DLT destination).

## Run

```bash
uv run --with-requirements requirements.txt dlt_fetch_timecamp.py \
   --from 2025-12-01 --to 2025-12-15 \
   --datasets entries,tasks,users,computer_activities,application_names \
   --format jsonl \
   --output ./output
```

## Available Datasets

| Dataset | Description |
|---------|-------------|
| `entries` | Time entries with project/task details |
| `tasks` | Projects & tasks hierarchy with breadcrumb paths and details |
| `computer_activities` | Desktop app tracking data |
| `users` | User details with group information |
| `application_names` | Application lookup table with names and categories |

## Todo

- [x] add application names
- [ ] fix tags in entries
- [ ] fix warnings

## License

MIT
