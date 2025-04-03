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
    try:
        client.get_table(table_ref)
        logger.info(f"Table exists: {table_ref}")
    except Exception:
        logger.info(f"Creating table: {table_ref}")
        
        # Define schema based on TimeCamp time entries format
        schema = [
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
            bigquery.SchemaField("hasEntryLocationHistory", "BOOLEAN")
        ]
        
        # Create table
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logger.info(f"Table created: {table_ref}")
    
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
        merge_sql = f"""
        MERGE `{table_ref}` T
        USING `{temp_table_ref}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET 
            duration = S.duration,
            user_id = S.user_id,
            user_name = S.user_name,
            task_id = S.task_id,
            task_note = S.task_note,
            last_modify = S.last_modify,
            date = S.date,
            start_time = S.start_time,
            end_time = S.end_time,
            locked = S.locked,
            name = S.name,
            addons_external_id = S.addons_external_id,
            billable = S.billable,
            invoiceId = S.invoiceId,
            color = S.color,
            description = S.description,
            hasEntryLocationHistory = S.hasEntryLocationHistory
        WHEN NOT MATCHED THEN
          INSERT ROW
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