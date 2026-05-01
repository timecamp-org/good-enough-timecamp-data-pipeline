# Fetch Project Data to S3

Use the same `jsonl` file format as the local project budget report and change
only the filesystem target.

```sh
# You can put these in .env instead of exporting them in the shell.
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="your-access-key-id"
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME="eu-central-1"

uv run --with-requirements requirements.txt dlt_fetch_timecamp.py \
   --from 2020-01-01 --to 2026-05-01 \
   --datasets entries,tasks \
   --format jsonl \
   --output s3://your-bucket/timecamp-output
```

The bucket must already exist.

## Query S3 With DuckDB

To query S3 directly from DuckDB, load `httpfs` and configure S3 credentials
before running the report query:

```sql
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET timecamp_s3 (
    TYPE S3,
    KEY_ID 'your-access-key-id',
    SECRET 'your-secret-access-key',
    REGION 'eu-central-1'
);
```

With this target, the project budget report paths become:

```sql
FROM read_json_auto('s3://your-bucket/timecamp-output/timecamp/tasks.*.jsonl')
FROM read_json_auto('s3://your-bucket/timecamp-output/timecamp/entries.*.jsonl')
```
