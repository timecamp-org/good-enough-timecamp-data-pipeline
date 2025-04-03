# TimeCamp Data Pipeline

A data pipeline to extract time entries from TimeCamp and load them into Google BigQuery.

![TimeCamp Data Pipeline Screenshot](docs/screenshot.png)

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
| id | INTEGER | Unique identifier for the time entry |
| duration | STRING | Duration of the time entry in seconds |
| user_id | STRING | Identifier of the user who created the entry |
| user_name | STRING | Name of the user who created the entry |
| task_id | STRING | Identifier of the task associated with the entry |
| task_note | STRING | Notes added to the task |
| last_modify | STRING | Timestamp of the last modification |
| date | DATE | Date when the time was recorded |
| start_time | STRING | Start time of the entry (HH:MM:SS) |
| end_time | STRING | End time of the entry (HH:MM:SS) |
| locked | STRING | Whether the entry is locked for editing |
| name | STRING | Name of the task |
| addons_external_id | STRING | External identifier for integrations |
| billable | INTEGER | Whether the entry is billable (1) or not (0) |
| invoiceId | STRING | Invoice identifier if the entry has been invoiced |
| color | STRING | Color code associated with the task |
| description | STRING | Description of the time entry |
| hasEntryLocationHistory | BOOLEAN | Whether location data was tracked |
| project_id | INTEGER | Identifier of the project (when include_project=true) |
| project_name | STRING | Name of the project (when include_project=true) |
| total_cost | FLOAT | Total cost of the time entry (when include_rates=true) |
| total_income | FLOAT | Total income of the time entry (when include_rates=true) |
| rate_income | FLOAT | Income rate of the time entry (when include_rates=true) |
| tags | JSON | Tag information for the time entry as a JSON object |
| breadcrumps | STRING | Path information showing hierarchy (when opt_fields includes breadcrumps) |
| email | STRING | Email address of the user who created the entry |
| group_name | STRING | Name of the user's primary group |
| group_breadcrumb_level_1 | STRING | Top level in the group hierarchy path |
| group_breadcrumb_level_2 | STRING | Second level in the group hierarchy path |
| group_breadcrumb_level_3 | STRING | Third level in the group hierarchy path |
| group_breadcrumb_level_4 | STRING | Fourth level in the group hierarchy path |

This schema supports the upsert pattern described in the previous section. When new data is loaded, it will:
1. Update all fields for any existing record with the same ID
2. Insert new records for any IDs that don't already exist in the table

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

Note: Project data, rates, and additional fields (tags, breadcrumps) are now always included by default in every API request. This ensures comprehensive data collection without requiring additional parameters.

### User Group Hierarchy Information

The data pipeline now includes detailed user information with each time entry:

- **Email**: The email address of the user who logged the time entry
- **Group structure**: Complete hierarchy information about the user's group, including:
  - The immediate group name of the user
  - Up to 4 levels of the group hierarchy path, from top-level organization down to the user's specific group

This group hierarchy information can be valuable for:
- Organizational analysis (comparing time usage across departments)
- Team-based reporting
- Role-based time tracking analysis
- Hierarchical department cost tracking

The breadcrumb levels represent the path through the organization structure, for example:
```
Level 1: "Company"
Level 2: "Sales & Marketing"
Level 3: "Marketing"
Level 4: "SEO"
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