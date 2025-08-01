#!/usr/bin/env python3
"""
Data-agnostic S3 destination for uploading JSON/JSONL data as weekly Parquet files.

This script works with any JSON/JSONL data that contains date fields and uploads 
it to AWS S3 or S3-compatible services (MinIO, Ceph, etc.) as compressed Parquet files
organized by ISO week.

Usage examples:
    # Basic usage with TimeCamp data
    python destination_aws_s3.py --from "2023-04-01" --to "2023-04-30"
    
    # Custom date column and input file
    python destination_aws_s3.py --from "2023-01-01" --to "2023-01-31" \\
        --date-column-name "created_at" --input events.jsonl
    
    # With MinIO (S3-compatible service)
    S3_ENDPOINT_URL=http://localhost:9000 \\
    python destination_aws_s3.py --from "2023-04-01" --to "2023-04-30"

Features:
- Data agnostic: works with any JSON/JSONL data containing dates
- Smart date parsing: extracts YYYY-MM-DD from various date formats
- Intelligent data type inference for optimal Parquet compression
- S3-compatible service support (MinIO, Ceph, DigitalOcean Spaces, etc.)
- Weekly file organization using ISO week numbers
"""
import os
import json
import argparse
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from common.logger import setup_logger
from common.utils import parse_date

def setup_s3_client(aws_access_key_id=None, aws_secret_access_key=None, aws_region=None, 
                     endpoint_url=None, use_path_style=False):
    """Set up the S3 client for AWS S3 or S3-compatible services (like MinIO).
    
    Args:
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key  
        aws_region: AWS region
        endpoint_url: Custom endpoint URL for S3-compatible services (e.g., MinIO)
        use_path_style: Use path-style addressing (required for MinIO and some S3-compatible services)
        
    Returns:
        S3 client object
    """
    # Prepare client configuration
    client_config = {
        'region_name': aws_region or 'us-east-1'
    }
    
    # Add credentials if provided
    if aws_access_key_id and aws_secret_access_key:
        client_config.update({
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key
        })
    
    # Add custom endpoint for S3-compatible services
    if endpoint_url:
        client_config['endpoint_url'] = endpoint_url
    
    # Configure addressing style for S3-compatible services
    if use_path_style or endpoint_url:
        from botocore.config import Config
        client_config['config'] = Config(s3={'addressing_style': 'path'})
    
    s3_client = boto3.client('s3', **client_config)
    return s3_client

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

def extract_date_from_string(date_string):
    """Extract YYYY-MM-DD date from a date string.
    
    Args:
        date_string: Date string (can include time components)
        
    Returns:
        Date string in YYYY-MM-DD format or None if invalid
    """
    if not date_string:
        return None
    
    # Convert to string if not already
    date_str = str(date_string).strip()
    
    # Extract YYYY-MM-DD part (first 10 characters if it matches the pattern)
    if len(date_str) >= 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str[:10]
    
    # Try to parse and reformat if it's a different format
    try:
        # Try common formats
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', 
                   '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%SZ', '%d/%m/%Y', '%m/%d/%Y']:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
    except Exception:
        pass
    
    return None

def filter_data_by_date_range(data, from_date, to_date, date_column, logger):
    """Filter data within a specific date range.
    
    Args:
        data: List of records
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format
        date_column: Name of the date column in the data
        logger: Logger object
        
    Returns:
        Filtered data
    """
    logger.info(f"Filtering data from {from_date} to {to_date} using column '{date_column}'")
    
    filtered_data = []
    from_date_obj = datetime.strptime(from_date, '%Y-%m-%d').date()
    to_date_obj = datetime.strptime(to_date, '%Y-%m-%d').date()
    
    for record in data:
        if date_column not in record:
            logger.warning(f"Skipping record without '{date_column}' field: {record.get('id', 'N/A')}")
            continue
        
        # Extract and normalize the date
        extracted_date = extract_date_from_string(record[date_column])
        if not extracted_date:
            logger.warning(f"Skipping record with invalid date in '{date_column}': {record.get(date_column, 'N/A')}")
            continue
        
        try:
            record_date = datetime.strptime(extracted_date, '%Y-%m-%d').date()
            if from_date_obj <= record_date <= to_date_obj:
                filtered_data.append(record)
        except ValueError:
            logger.warning(f"Skipping record with unparseable date: {extracted_date}")
            continue
    
    logger.info(f"Filtered to {len(filtered_data)} records in date range {from_date} to {to_date}")
    return filtered_data

def group_data_by_week(data, date_column, logger):
    """Group data by ISO week using specified date column.
    
    Args:
        data: List of records
        date_column: Name of the date column in the data
        logger: Logger object
        
    Returns:
        Dictionary with week keys and corresponding data
    """
    logger.info(f"Grouping data by week using column '{date_column}'")
    
    weekly_data = {}
    
    for record in data:
        if date_column not in record:
            logger.warning(f"Skipping record without '{date_column}' field: {record.get('id', 'N/A')}")
            continue
        
        # Extract and normalize the date
        extracted_date = extract_date_from_string(record[date_column])
        if not extracted_date:
            logger.warning(f"Skipping record with invalid date in '{date_column}': {record.get(date_column, 'N/A')}")
            continue
        
        try:
            # Parse the date and get ISO week
            date_obj = datetime.strptime(extracted_date, '%Y-%m-%d')
            year, week, _ = date_obj.isocalendar()
            week_key = f"{year}_W{week:02d}"
            
            if week_key not in weekly_data:
                weekly_data[week_key] = []
            
            weekly_data[week_key].append(record)
            
        except ValueError:
            logger.warning(f"Skipping record with unparseable date: {extracted_date}")
            continue
    
    logger.info(f"Grouped data into {len(weekly_data)} weeks: {list(weekly_data.keys())}")
    return weekly_data

def convert_to_parquet_and_upload(weekly_data, s3_client, bucket_name, s3_prefix, logger, 
                                   endpoint_url=None):
    """Convert weekly data to Parquet format and upload to S3.
    
    Args:
        weekly_data: Dictionary of weekly data
        s3_client: S3 client object
        bucket_name: S3 bucket name
        s3_prefix: S3 key prefix
        logger: Logger object
        endpoint_url: Custom endpoint URL (for S3-compatible services)
    """
    if endpoint_url:
        logger.info(f"Converting and uploading {len(weekly_data)} weekly files to S3-compatible service at {endpoint_url}")
    else:
        logger.info(f"Converting and uploading {len(weekly_data)} weekly files to AWS S3")
    
    for week_key, week_records in weekly_data.items():
        try:
            logger.info(f"Processing week {week_key} with {len(week_records)} records")
            
            # Convert to DataFrame
            df = pd.DataFrame(week_records)
            
            # Ensure consistent data types for better Parquet compression
            # Infer and optimize data types automatically
            for col in df.columns:
                # Skip empty columns
                if df[col].isna().all():
                    continue
                
                # Try to convert to numeric (int or float)
                numeric_converted = pd.to_numeric(df[col], errors='coerce')
                if not numeric_converted.isna().all():
                    # If most values can be converted to numeric, use it
                    if numeric_converted.notna().sum() / len(df) > 0.8:
                        df[col] = numeric_converted
                        continue
                
                # Try to convert to datetime/date
                if col.lower() in ['date', 'time', 'timestamp', 'created', 'updated', 'modified']:
                    try:
                        date_converted = pd.to_datetime(df[col], errors='coerce')
                        if not date_converted.isna().all():
                            # If most values can be converted to datetime, use date only
                            if date_converted.notna().sum() / len(df) > 0.8:
                                df[col] = date_converted.dt.date
                                continue
                    except:
                        pass
                
                # Try to convert to boolean
                if df[col].dtype == 'object':
                    unique_values = df[col].dropna().unique()
                    if len(unique_values) <= 2 and all(str(v).lower() in ['true', 'false', '1', '0', 'yes', 'no'] for v in unique_values):
                        try:
                            df[col] = df[col].map(lambda x: str(x).lower() in ['true', '1', 'yes'] if pd.notna(x) else x).astype('boolean')
                            continue
                        except:
                            pass
                
                # Keep as string (object) type for everything else
                df[col] = df[col].astype('string')
            
            # Convert to PyArrow table for better control over Parquet writing
            table = pa.Table.from_pandas(df)
            
            # Create temporary file to write Parquet
            temp_file = f"temp_timecamp_data_{week_key}.parquet"
            
            try:
                # Write Parquet with Gzip compression
                pq.write_table(
                    table, 
                    temp_file,
                    compression='gzip',
                    use_dictionary=True,  # Use dictionary encoding for better compression
                    write_statistics=True  # Include statistics for query optimization
                )
                
                # Upload to S3
                s3_key = f"{s3_prefix.rstrip('/')}/timecamp_data_{week_key}.parquet"
                
                logger.info(f"Uploading {temp_file} to s3://{bucket_name}/{s3_key}")
                
                # Prepare upload arguments
                extra_args = {'ContentType': 'application/octet-stream'}
                
                # Add server-side encryption for AWS S3 (may not be supported by all S3-compatible services)
                if not endpoint_url:
                    extra_args['ServerSideEncryption'] = 'AES256'
                
                try:
                    s3_client.upload_file(
                        temp_file,
                        bucket_name,
                        s3_key,
                        ExtraArgs=extra_args
                    )
                except Exception as upload_error:
                    # Retry without server-side encryption for S3-compatible services
                    if endpoint_url and 'ServerSideEncryption' in str(upload_error):
                        logger.warning(f"Server-side encryption not supported, retrying without encryption")
                        extra_args.pop('ServerSideEncryption', None)
                        s3_client.upload_file(
                            temp_file,
                            bucket_name,
                            s3_key,
                            ExtraArgs=extra_args
                        )
                    else:
                        raise upload_error
                
                logger.info(f"Successfully uploaded week {week_key} ({len(week_records)} records)")
                
            finally:
                # Clean up temporary file
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
                    
        except Exception as e:
            logger.error(f"Error processing week {week_key}: {str(e)}")
            raise

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Upload data to AWS S3 as weekly Parquet files')
    parser.add_argument('--from', dest='from_date', required=True, 
                       help='Start date for filtering data (YYYY-MM-DD) - REQUIRED')
    parser.add_argument('--to', dest='to_date', required=True,
                       help='End date for filtering data (YYYY-MM-DD) - REQUIRED')
    parser.add_argument('--date-column-name', dest='date_column', default='date',
                       help='Name of the date column in the data (default: date)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--input', help='Input file path (default: auto-detect timecamp_data.jsonl or .json)')
    
    args = parser.parse_args()
    
    # Set up logger
    logger = setup_logger('s3_upload', debug=args.debug)
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_prefix = os.getenv("S3_PREFIX", "timecamp/weekly")
    
    # S3-compatible service configuration (e.g., MinIO)
    endpoint_url = os.getenv("S3_ENDPOINT_URL")  # e.g., http://localhost:9000 for MinIO
    use_path_style = os.getenv("S3_USE_PATH_STYLE", "false").lower() in ("true", "1", "yes")
    
    # Validate required parameters
    if not bucket_name:
        logger.error("S3 bucket name is required. Set S3_BUCKET_NAME in your .env file.")
        exit(1)
    
    # Check for input file (prefer JSONL)
    input_file = args.input
    if not input_file:
        input_file = "timecamp_data.jsonl"
        if not os.path.exists(input_file):
            input_file = "timecamp_data.json"
    
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        exit(1)
    
    # Parse and validate date parameters
    try:
        from_date = parse_date(args.from_date)
        to_date = parse_date(args.to_date)
        logger.info(f"Processing data from {from_date} to {to_date} using date column '{args.date_column}'")
    except ValueError as e:
        logger.error(f"Invalid date format: {str(e)}")
        exit(1)
    
    # Validate date range
    from_date_obj = datetime.strptime(from_date, '%Y-%m-%d').date()
    to_date_obj = datetime.strptime(to_date, '%Y-%m-%d').date()
    
    if from_date_obj > to_date_obj:
        logger.error(f"Start date ({from_date}) cannot be after end date ({to_date})")
        exit(1)
    
    try:
        # Read data from file
        data = read_json_data(input_file, logger)
        
        # Filter data by date range
        data = filter_data_by_date_range(data, from_date, to_date, args.date_column, logger)
        
        if not data:
            logger.warning("No data to process after filtering")
            return
        
        # Group data by week
        weekly_data = group_data_by_week(data, args.date_column, logger)
        
        if not weekly_data:
            logger.warning("No weekly data to upload")
            return
        
        # Set up S3 client
        s3_client = setup_s3_client(
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_region,
            endpoint_url,
            use_path_style
        )
        
        # Convert to Parquet and upload to S3
        convert_to_parquet_and_upload(weekly_data, s3_client, bucket_name, s3_prefix, logger, endpoint_url)
        
        logger.info(f"Successfully uploaded {len(weekly_data)} weekly files to S3 bucket: {bucket_name}")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()