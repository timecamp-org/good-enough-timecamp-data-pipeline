#!/usr/bin/env python3
import os
import json
import time
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path
from dotenv import load_dotenv
from common.logger import setup_logger

def setup_bigquery_client(credentials_path=None):
    """Set up the BigQuery client.
    
    Args:
        credentials_path: Path to service account JSON file
        
    Returns:
        BigQuery client object
    """
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=credentials)
    else:
        # Use default credentials (ADC)
        client = bigquery.Client()
    
    return client

def read_json_data(file_path, logger):
    """Read data from a JSON or JSONL file.
    
    Args:
        file_path: Path to the JSON or JSONL file
        logger: Logger object
        
    Returns:
        Data from the file
    """
    logger.info(f"Reading data from {file_path}")
    
    try:
        # Check if file is JSONL (ends with .jsonl)
        if file_path.endswith('.jsonl'):
            # Read JSONL file (one JSON object per line)
            data = []
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():  # Skip empty lines
                        data.append(json.loads(line))
        else:
            # Read standard JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
        
        logger.info(f"Read {len(data)} records from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error reading file: {str(e)}")
        raise

def upload_to_bigquery(data, client, project_id, dataset_id, table_id, input_file, logger):
    """Upload data to BigQuery using upsert pattern."""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Check if table exists
    table_exists = False
    try:
        client.get_table(table_ref)
        logger.info(f"Table exists: {table_ref}")
        table_exists = True
    except Exception:
        logger.info(f"Table does not exist: {table_ref}")
    
    # Always drop and recreate the table to ensure schema is correct
    if table_exists:
        logger.info(f"Dropping existing table to update schema: {table_ref}")
        try:
            client.delete_table(table_ref)
            logger.info(f"Table dropped: {table_ref}")
            table_exists = False
        except Exception as e:
            logger.error(f"Error dropping table: {str(e)}")
            raise
    
    # Define schema based on TimeCamp time entries format
    # Including fields for project data, rates, tags, and breadcrumbs
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
        
        # Tags information (as JSON string)
        bigquery.SchemaField("tags", "STRING"),
        
        # Path information
        bigquery.SchemaField("breadcrumps", "STRING"),
    ]
    
    # Create the table
    logger.info(f"Creating table: {table_ref}")
    table = bigquery.Table(table_ref, schema=schema)
    try:
        client.create_table(table)
        logger.info(f"Table created: {table_ref}")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise
    
    # Create a temporary table for the upsert
    temp_table_id = f"temp_{table_id}_{int(time.time())}"
    temp_table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"
    logger.info(f"Creating temporary table: {temp_table_ref}")
    
    # Configure job for loading data
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    try:
        # Load data to temporary table
        if input_file.endswith('.jsonl'):
            # Load directly from JSONL
            with open(input_file, 'rb') as source_file:
                load_job = client.load_table_from_file(
                    source_file,
                    f"{project_id}.{dataset_id}.{temp_table_id}",
                    job_config=job_config
                )
                load_job.result()
        else:
            # Convert JSON to JSONL first
            with open(input_file, 'r') as f:
                nl_json = "\n".join(json.dumps(record) for record in data)
            
            # Create a temporary file
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w+', suffix='.jsonl', delete=False) as temp_file:
                temp_file_path = temp_file.name
                temp_file.write(nl_json)
                temp_file.flush()
            
            try:
                # Load from temporary file
                with open(temp_file_path, 'rb') as source_file:
                    load_job = client.load_table_from_file(
                        source_file,
                        f"{project_id}.{dataset_id}.{temp_table_id}",
                        job_config=job_config
                    )
                    load_job.result()
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)
        
        logger.info(f"Data loaded to temporary table ({len(data)} records)")
        
        # Perform merge operation (upsert)
        # This MERGE SQL statement performs an upsert operation:
        # 1. When a record with the same ID exists, it updates all fields with new values
        # 2. When a record with the ID doesn't exist, it inserts a new row with all fields
        # This ensures we maintain historical data while updating existing records.
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
            T.tags = TO_JSON_STRING(S.tags),
            T.breadcrumps = CAST(S.breadcrumps AS STRING)
        WHEN NOT MATCHED THEN
          INSERT (
            id, duration, user_id, user_name, task_id, task_note,
            last_modify, date, start_time, end_time, locked, name,
            addons_external_id, billable, invoiceId, color, description,
            hasEntryLocationHistory, project_id, project_name, total_cost,
            total_income, rate_income, tags, breadcrumps
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
            TO_JSON_STRING(S.tags),
            CAST(S.breadcrumps AS STRING)
          )
        """
        
        logger.info("Performing MERGE operation")
        merge_job = client.query(merge_sql)
        merge_job.result()
        
        logger.info("MERGE operation completed successfully")
        
    finally:
        # Always clean up the temporary table
        try:
            client.delete_table(temp_table_ref)
            logger.info(f"Temporary table deleted: {temp_table_ref}")
        except Exception as e:
            logger.warning(f"Failed to delete temporary table: {str(e)}")

def main():
    """Main function."""
    # Set up logger
    logger = setup_logger('bigquery_upload', debug=False)
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    dataset_id = os.getenv("BIGQUERY_DATASET")
    table_id = os.getenv("BIGQUERY_TABLE")
    
    # Check for input file (prefer JSONL)
    input_file = "timecamp_data.jsonl"
    if not os.path.exists(input_file):
        input_file = "timecamp_data.json"
    
    # Validate required parameters
    if not project_id:
        logger.error("Project ID is required. Set GOOGLE_CLOUD_PROJECT in your .env file.")
        exit(1)
    
    if not dataset_id:
        logger.error("Dataset ID is required. Set BIGQUERY_DATASET in your .env file.")
        exit(1)
    
    if not table_id:
        logger.error("Table ID is required. Set BIGQUERY_TABLE in your .env file.")
        exit(1)
    
    try:
        # Read data from file
        data = read_json_data(input_file, logger)
        
        # Set up BigQuery client
        client = setup_bigquery_client(credentials_path)
        
        # Upload data to BigQuery using upsert pattern
        upload_to_bigquery(data, client, project_id, dataset_id, table_id, input_file, logger)
        
        logger.info("Data successfully uploaded to BigQuery")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main() 