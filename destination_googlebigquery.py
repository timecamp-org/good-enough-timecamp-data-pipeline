#!/usr/bin/env python3
import os
import json
import time
import argparse
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from pathlib import Path
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

def setup_logger(name: str = 'bigquery_upload', debug: bool = False) -> logging.Logger:
    """Set up and return a logger instance."""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        os.makedirs('logs', exist_ok=True)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler = RotatingFileHandler(
            'logs/bigquery_upload.log',
            maxBytes=10*1024*1024,
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

def setup_clients(credentials_path):
    """Set up BigQuery and Storage clients."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    bq_client = bigquery.Client(credentials=credentials)
    storage_client = storage.Client(credentials=credentials)
    return bq_client, storage_client

def list_gcs_files(storage_client, bucket_name, prefix="", logger=None):
    """List files in GCS bucket with optional prefix filter."""
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    files = []
    for blob in blobs:
        files.append({
            'name': blob.name,
            'size': blob.size,
            'created': blob.time_created,
            'updated': blob.updated
        })
    
    if logger:
        logger.info(f"Found {len(files)} files in gs://{bucket_name}/{prefix}")
    
    return files

def get_latest_file(storage_client, bucket_name, prefix="timecamp_incremental_data", logger=None):
    """Get the most recently created file matching the prefix."""
    files = list_gcs_files(storage_client, bucket_name, prefix, logger)
    
    if not files:
        raise FileNotFoundError(f"No files found with prefix '{prefix}' in bucket '{bucket_name}'")
    
    # Sort by creation time, get latest
    latest_file = max(files, key=lambda x: x['created'])
    
    if logger:
        logger.info(f"Latest file: {latest_file['name']} (created: {latest_file['created']})")
    
    return latest_file['name']

def download_and_convert_gcs_file(storage_client, bucket_name, file_name, logger):
    """Download file from GCS and convert to format suitable for BigQuery."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    logger.info(f"Downloading gs://{bucket_name}/{file_name}")
    
    # Download content as string
    content = blob.download_as_text()
    
    # Determine file format and parse accordingly
    if file_name.endswith('.csv'):
        # Convert CSV to JSON format
        import csv
        import io
        
        data = []
        csv_reader = csv.DictReader(io.StringIO(content))
        for row in csv_reader:
            # Convert empty strings to None for proper BigQuery handling
            cleaned_row = {}
            for key, value in row.items():
                if value == '':
                    cleaned_row[key] = None
                else:
                    # Try to parse JSON strings back to objects (for tags field)
                    if key == 'tags' and value and value.startswith('{'):
                        try:
                            cleaned_row[key] = json.loads(value)
                        except json.JSONDecodeError:
                            cleaned_row[key] = value
                    else:
                        cleaned_row[key] = value
            data.append(cleaned_row)
                
    elif file_name.endswith('.jsonl'):
        # Parse JSONL
        data = []
        for line in content.strip().split('\n'):
            if line.strip():
                data.append(json.loads(line))
                
    elif file_name.endswith('.json'):
        # Parse JSON
        data = json.loads(content)
    else:
        raise ValueError(f"Unsupported file format: {file_name}")
    
    logger.info(f"Parsed {len(data)} records from {file_name}")
    return data

def upload_to_bigquery(data, bq_client, project_id, dataset_id, table_id, logger):
    """Upload data to BigQuery using upsert pattern."""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Check if table exists
    table_exists = False
    existing_table = None
    try:
        existing_table = bq_client.get_table(table_ref)
        logger.info(f"Table exists: {table_ref}")
        table_exists = True
    except Exception:
        logger.info(f"Table does not exist: {table_ref}")

    # Define schema based on TimeCamp time entries format
    schema = [
        # Core time entry fields
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("duration", "STRING"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("user_name", "STRING"),
        bigquery.SchemaField("task_id", "STRING"),
        bigquery.SchemaField("task_note", "STRING"),
        bigquery.SchemaField("last_modify", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("start_time", "STRING"),
        bigquery.SchemaField("end_time", "STRING"),
        bigquery.SchemaField("locked", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("addons_external_id", "STRING"),
        bigquery.SchemaField("billable", "INTEGER"),
        bigquery.SchemaField("invoiceId", "STRING"),
        bigquery.SchemaField("color", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("hasEntryLocationHistory", "BOOLEAN"),
        # Project-related fields
        bigquery.SchemaField("project_id", "INTEGER"),
        bigquery.SchemaField("project_name", "STRING"),
        # Rate-related fields
        bigquery.SchemaField("total_cost", "FLOAT"),
        bigquery.SchemaField("total_income", "FLOAT"),
        bigquery.SchemaField("rate_income", "FLOAT"),
        # Tags information (as JSON)
        bigquery.SchemaField("tags", "JSON"),
        # Path information
        bigquery.SchemaField("breadcrumps", "STRING"),
        # User info columns
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("group_name", "STRING"),
        bigquery.SchemaField("group_breadcrumb_level_1", "STRING"),
        bigquery.SchemaField("group_breadcrumb_level_2", "STRING"),
        bigquery.SchemaField("group_breadcrumb_level_3", "STRING"),
        bigquery.SchemaField("group_breadcrumb_level_4", "STRING"),
    ]

    # Check if we need to update the schema
    schema_needs_update = False
    if table_exists:
        existing_fields = {
            field.name: field.field_type for field in existing_table.schema
        }
        new_fields = {field.name: field.field_type for field in schema}

        for name, field_type in new_fields.items():
            if name not in existing_fields or existing_fields[name] != field_type:
                schema_needs_update = True
                logger.info(f"Schema change detected: field '{name}' needs update")
                break

        if schema_needs_update:
            logger.info(f"Dropping existing table to update schema: {table_ref}")
            try:
                bq_client.delete_table(table_ref)
                logger.info(f"Table dropped: {table_ref}")
                table_exists = False
            except Exception as e:
                logger.error(f"Error dropping table: {str(e)}")
                raise

    # Create the table if it doesn't exist
    if not table_exists:
        logger.info(f"Creating table: {table_ref}")
        table = bigquery.Table(table_ref, schema=schema)
        try:
            bq_client.create_table(table)
            logger.info(f"Table created: {table_ref}")
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise

    # Create a temporary table for the upsert
    temp_table_id = f"temp_{table_id}_{int(time.time())}"
    temp_table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"
    logger.info(f"Creating temporary table: {temp_table_ref}")

    # Convert data to JSONL format for BigQuery loading
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False, encoding='utf-8') as temp_file:
        temp_file_path = temp_file.name
        for record in data:
            temp_file.write(json.dumps(record, ensure_ascii=False) + "\n")
        temp_file.flush()

    # Configure job for loading data
    job_config = bigquery.LoadJobConfig(
        autodetect=False,  # Disable autodetect to use our explicit schema
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    try:
        # Load data to temporary table
        with open(temp_file_path, "rb") as source_file:
            load_job = bq_client.load_table_from_file(
                source_file,
                f"{project_id}.{dataset_id}.{temp_table_id}",
                job_config=job_config,
            )
            load_job.result()

        logger.info(f"Data loaded to temporary table ({len(data)} records)")

        # Perform merge operation (upsert)
        merge_sql = f"""
        MERGE `{table_ref}` T
        USING `{temp_table_ref}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET 
            T.duration = CAST(S.duration AS STRING),
            T.user_id = CAST(S.user_id AS STRING),
            T.user_name = CAST(S.user_name AS STRING),
            T.task_id = CAST(S.task_id AS STRING),
            T.task_note = CAST(S.task_note AS STRING),
            T.last_modify = CAST(S.last_modify AS STRING),
            T.date = CAST(S.date AS DATE),
            T.start_time = CAST(S.start_time AS STRING),
            T.end_time = CAST(S.end_time AS STRING),
            T.locked = CAST(S.locked AS STRING),
            T.name = CAST(S.name AS STRING),
            T.addons_external_id = CAST(S.addons_external_id AS STRING),
            T.billable = CAST(S.billable AS INTEGER),
            T.invoiceId = CAST(S.invoiceId AS STRING),
            T.color = CAST(S.color AS STRING),
            T.description = CAST(S.description AS STRING),
            T.hasEntryLocationHistory = CAST(S.hasEntryLocationHistory AS BOOLEAN),
            T.project_id = CAST(S.project_id AS INTEGER),
            T.project_name = CAST(S.project_name AS STRING),
            T.total_cost = CAST(S.total_cost AS FLOAT64),
            T.total_income = CAST(S.total_income AS FLOAT64),
            T.rate_income = CAST(S.rate_income AS FLOAT64),
            T.tags = S.tags,
            T.breadcrumps = CAST(S.breadcrumps AS STRING),
            T.email = CAST(S.email AS STRING),
            T.group_name = CAST(S.group_name AS STRING),
            T.group_breadcrumb_level_1 = CAST(S.group_breadcrumb_level_1 AS STRING),
            T.group_breadcrumb_level_2 = CAST(S.group_breadcrumb_level_2 AS STRING),
            T.group_breadcrumb_level_3 = CAST(S.group_breadcrumb_level_3 AS STRING),
            T.group_breadcrumb_level_4 = CAST(S.group_breadcrumb_level_4 AS STRING)
        WHEN NOT MATCHED THEN
          INSERT (
            id, duration, user_id, user_name, task_id, task_note,
            last_modify, date, start_time, end_time, locked, name,
            addons_external_id, billable, invoiceId, color, description,
            hasEntryLocationHistory, project_id, project_name, total_cost,
            total_income, rate_income, tags, breadcrumps, email, group_name,
            group_breadcrumb_level_1, group_breadcrumb_level_2, group_breadcrumb_level_3,
            group_breadcrumb_level_4
          )
          VALUES (
            CAST(S.id AS INTEGER),
            CAST(S.duration AS STRING),
            CAST(S.user_id AS STRING),
            CAST(S.user_name AS STRING),
            CAST(S.task_id AS STRING),
            CAST(S.task_note AS STRING),
            CAST(S.last_modify AS STRING),
            CAST(S.date AS DATE),
            CAST(S.start_time AS STRING),
            CAST(S.end_time AS STRING),
            CAST(S.locked AS STRING),
            CAST(S.name AS STRING),
            CAST(S.addons_external_id AS STRING),
            CAST(S.billable AS INTEGER),
            CAST(S.invoiceId AS STRING),
            CAST(S.color AS STRING),
            CAST(S.description AS STRING),
            CAST(S.hasEntryLocationHistory AS BOOLEAN),
            CAST(S.project_id AS INTEGER),
            CAST(S.project_name AS STRING),
            CAST(S.total_cost AS FLOAT64),
            CAST(S.total_income AS FLOAT64),
            CAST(S.rate_income AS FLOAT64),
            S.tags,
            CAST(S.breadcrumps AS STRING),
            CAST(S.email AS STRING),
            CAST(S.group_name AS STRING),
            CAST(S.group_breadcrumb_level_1 AS STRING),
            CAST(S.group_breadcrumb_level_2 AS STRING),
            CAST(S.group_breadcrumb_level_3 AS STRING),
            CAST(S.group_breadcrumb_level_4 AS STRING)
          )
        """

        logger.info("Performing MERGE operation")
        merge_job = bq_client.query(merge_sql)
        merge_result = merge_job.result()
        
        # Get merge statistics if available
        if hasattr(merge_job, 'num_dml_affected_rows'):
            logger.info(f"MERGE completed - Rows affected: {merge_job.num_dml_affected_rows}")
        
        logger.info("MERGE operation completed successfully")

    finally:
        # Clean up temporary table
        try:
            bq_client.delete_table(temp_table_ref)
            logger.info(f"Temporary table deleted: {temp_table_ref}")
        except Exception as e:
            logger.warning(f"Failed to delete temporary table: {str(e)}")
        
        # Clean up temporary file
        try:
            os.unlink(temp_file_path)
        except Exception as e:
            logger.warning(f"Failed to delete temporary file: {str(e)}")

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Load TimeCamp data from Google Cloud Storage to BigQuery"
    )
    parser.add_argument(
        "--bucket", 
        default="cia_good_enough_timecamp_data", 
        help="GCS bucket name"
    )
    parser.add_argument(
        "--file-name",
        default=None,
        help="Specific file name in GCS (if not provided, uses latest file)"
    )
    parser.add_argument(
        "--file-prefix",
        default="timecamp_incremental_data",
        help="File prefix to search for if --file-name not provided"
    )
    parser.add_argument(
        "--credentials",
        default="/service-account/dbt-mark_cia-data-6be89f0747c2.json",
        help="Path to service account JSON file"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--list-files", action="store_true", help="List available files in bucket and exit")

    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()
    
    # Set up logger
    logger = setup_logger("bigquery_upload", args.debug)

    # Hardcoded target table configuration
    project_id = "cia-data"
    dataset_id = "source_timecamp_v2"
    table_id = "good_enough_timecamp_data"

    # Validate service account file exists
    if not os.path.exists(args.credentials):
        logger.error(f"Service account file not found: {args.credentials}")
        logger.error("Please ensure the file exists in the current directory.")
        exit(1)

    try:
        # Set up clients
        bq_client, storage_client = setup_clients(args.credentials)
        
        # List files mode
        if args.list_files:
            files = list_gcs_files(storage_client, args.bucket, args.file_prefix, logger)
            logger.info("Available files:")
            for file_info in sorted(files, key=lambda x: x['created'], reverse=True):
                logger.info(f"  {file_info['name']} ({file_info['size']} bytes, created: {file_info['created']})")
            return

        # Determine which file to process
        if args.file_name:
            file_name = args.file_name
            logger.info(f"Using specified file: {file_name}")
        else:
            file_name = get_latest_file(storage_client, args.bucket, args.file_prefix, logger)
            logger.info(f"Using latest file: {file_name}")

        # Download and parse data from GCS
        data = download_and_convert_gcs_file(storage_client, args.bucket, file_name, logger)

        if not data:
            logger.warning("No data found in the file")
            return

        # Upload data to BigQuery
        upload_to_bigquery(data, bq_client, project_id, dataset_id, table_id, logger)

        logger.info("=== Summary ===")
        logger.info(f"Source: gs://{args.bucket}/{file_name}")
        logger.info(f"Destination: {project_id}.{dataset_id}.{table_id}")
        logger.info(f"Records processed: {len(data)}")
        logger.info("Data successfully uploaded to BigQuery!")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.error("Failed to upload data to BigQuery", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main()