# TimeCamp Data Pipeline

A data pipeline to extract TimeCamp data (time entries, computer activities) and load them into various desinations (Google Big Query, S3).

## Run

```bash
pip3 install -r requirements.txt
cp .env.sample .env

# fetch computer activities
python fetch_computer_time_data.py --from 2025-07-10 --to 2025-07-12 --debug # optional params: --user-ids "640"

# fetch time entries
python fetch_timsheet_data.py --from "2023-04-01" --to "2023-04-30" --debug #optional params: --output custom_filename.jsonl

# export to S3
python destination_aws_s3.py --from 2025-06-01 --to 2025-06-16 --date-column-name "start_time" --input timecamp_computer_time_data.jsonl

# export to Google Big Query
python destination_googlebigquery.py

# convert jsonl to CSV if needed
python jsonl_to_csv.py timecamp_computer_time_data.jsonl 
```

## Google BigQuery Configuration Destination

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

The script will automatically create the table if it doesn't exist, but you must create the dataset manually:

1. Go to the BigQuery console in Google Cloud
2. Select your project
3. Click "Create Dataset"
4. Enter the dataset ID that matches your `BIGQUERY_DATASET` value
5. Select appropriate location and settings for your needs
6. Click "Create Dataset"

## AWS S3 and S3-Compatible Services Configuration Destination

The S3 destination supports both AWS S3 and S3-compatible services like MinIO, Ceph, or DigitalOcean Spaces.

1. Create an AWS account if you don't have one already
2. Create an S3 bucket where the Parquet files will be stored
3. Set up AWS credentials with the following permissions for the target bucket:
   - `s3:PutObject` - Allows uploading files to the bucket
   - `s3:PutObjectAcl` - Allows setting object permissions
   - `s3:GetObject` - Allows reading files from the bucket (optional, for verification)

## Crontab Setup

To automate the data pipeline, you can set up a cron job:

```bash
# Example: Run daily at 1:00 AM with BigQuery destination
0 1 * * * cd /path/to/repository && python fetch_timesheet_data.py && python destination_googlebigquery.py

# Example: Run daily at 1:00 AM with S3 destination (upload yesterday's data)
0 1 * * * cd /path/to/repository && python fetch_timesheet_data.py && python destination_aws_s3.py --from $(date -d "yesterday" +%Y-%m-%d) --to $(date -d "yesterday" +%Y-%m-%d)

# Example: Run weekly on Sundays at 2:00 AM with S3 destination (upload last 7 days)
0 2 * * 0 cd /path/to/repository && python fetch_timesheet_data.py --from "$(date -d '7 days ago' +%Y-%m-%d)" --to "$(date -d '1 day ago' +%Y-%m-%d)" && python destination_aws_s3.py --from $(date -d "7 days ago" +%Y-%m-%d) --to $(date -d "1 day ago" +%Y-%m-%d)
```

## License

MIT