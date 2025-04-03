# TimeCamp Data Pipeline

A data pipeline to extract time entries from TimeCamp and load them into Google BigQuery.

## Setup

1. Clone the repository
2. Install dependencies:
```bash
pip3 install -r requirements.txt
```
3. Copy `.env.sample` to `.env` and fill in your API keys and BigQuery configuration:
```bash
cp .env.sample .env
```

## Google BigQuery Configuration

To use the BigQuery integration, you need to:

1. Create a Google Cloud project if you don't have one already
2. Enable the BigQuery API in your project
3. Create a service account with the following permissions:
   - `roles/bigquery.dataEditor` - Allows creating and modifying tables and data
   - `roles/bigquery.user` - Allows using the BigQuery service
   - `roles/bigquery.jobUser` - Allows running BigQuery jobs (needed for data loading)
4. Create Key and download the service account JSON key file
5. Update the following in your `.env` file:
   - `GOOGLE_APPLICATION_CREDENTIALS`: Path to your service account JSON file
   - `GOOGLE_CLOUD_PROJECT`: Your Google Cloud project ID
   - `BIGQUERY_DATASET`: The BigQuery dataset ID where data will be stored
   - `BIGQUERY_TABLE`: The table name for TimeCamp data (default: timecamp_entries)

### Dataset Preparation

The script will automatically create the table if it doesn't exist, but you must create the dataset manually:

1. Go to the BigQuery console in Google Cloud
2. Select your project
3. Click "Create Dataset"
4. Enter the dataset ID that matches your `BIGQUERY_DATASET` value
5. Select appropriate location and settings for your needs
6. Click "Create Dataset"

## BigQuery Table Schema

The TimeCamp data is stored in BigQuery with the following schema:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| id | INTEGER | Primary key, used for upsert matching |
| duration | STRING | Duration of the time entry in seconds |
| user_id | STRING | ID of the user who logged the time |
| user_name | STRING | Name of the user who logged the time |
| task_id | STRING | ID of the task the time was logged against |
| task_note | STRING | Notes attached to the task |
| last_modify | STRING | Timestamp of when the entry was last modified |
| date | DATE | Date of the time entry |
| start_time | STRING | Start time of the entry (HH:MM:SS) |
| end_time | STRING | End time of the entry (HH:MM:SS) |
| locked | STRING | Whether the time entry is locked (0/1) |
| name | STRING | Name of the task |
| addons_external_id | STRING | External ID for integrations |
| billable | INTEGER | Whether the time entry is billable (0/1) |
| invoiceId | STRING | ID of associated invoice |
| color | STRING | Color code for the task |
| description | STRING | Description of the time entry |
| hasEntryLocationHistory | BOOLEAN | Whether location history exists |

During the upsert operation:
- Existing entries are identified by matching the `id` field
- If a match is found, all fields are updated with the latest values
- If no match is found, a new record is inserted with all fields

This ensures your BigQuery table always contains the most up-to-date time entry data from TimeCamp.

## Usage

### Fetching TimeCamp Data

```bash
# Fetch yesterday's entries (default, JSONL format)
python fetch_timecamp_data.py

# Choose output format (JSONL is optimized for BigQuery)
python fetch_timecamp_data.py --format json   # Pretty JSON format
python fetch_timecamp_data.py --format jsonl  # Newline-delimited JSON (default)

# Fetch entries for a specific date range
python fetch_timecamp_data.py --from "2023-04-01" --to "2023-04-30"

# Enable debug mode
python fetch_timecamp_data.py --debug

# Specify output file
python fetch_timecamp_data.py --output custom_filename.jsonl
```

### Uploading to BigQuery

```bash
# Upload data to BigQuery
python destination_googlebigquery.py
```

The BigQuery upload script:
- Uses the environment variables from your `.env` file
- Automatically detects and reads from `timecamp_data.jsonl` (or falls back to `timecamp_data.json`)
- Uses an upsert pattern to update existing records or insert new ones based on the entry ID
- Optimizes the upload process by directly loading JSONL files

#### Incremental Updates

The BigQuery upload process uses a sophisticated upsert (update/insert) pattern:

1. Data is first loaded into a temporary BigQuery table
2. A SQL MERGE operation is performed that:
   - Updates existing records if the entry ID already exists in the target table
   - Inserts new records if the entry ID doesn't exist yet
3. The temporary table is automatically deleted after the operation

This approach ensures that:
- Existing records are updated with the latest data from TimeCamp
- New records are added to the table
- No duplicate entries are created
- Historical data is preserved

## System Architecture

```mermaid
graph LR
    A[TimeCamp API] --> B[fetch_timecamp_data.py]
    B --> C[timecamp_data.jsonl]
    C --> D[destination_googlebigquery.py]
    D --> E[Google BigQuery]
```

## Crontab Setup

To automate the data pipeline, you can set up a cron job:

```bash
# Example: Run daily at 1:00 AM
0 1 * * * cd /path/to/repository && python fetch_timecamp_data.py && python destination_googlebigquery.py
```

## License

MIT