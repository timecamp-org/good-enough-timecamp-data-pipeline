# TimeCamp Data Pipeline

A data pipeline to extract TimeCamp datasets (time entries, computer activities, users, tasks) and load them into various desinations (Google Big Query, S3, CSV, JSONL, Parquet, any DLT destination).

## Run

```bash
uv run --with-requirements requirements.txt dlt_fetch_timecamp.py \
   --from 2025-12-01 --to 2025-12-15 \
   --datasets entries,tasks,computer_activities,users \
   --format jsonl \
   --output ./output

# Debug mode
python dlt_fetch_timecamp.py --datasets entries,users --debug
```

## License

MIT