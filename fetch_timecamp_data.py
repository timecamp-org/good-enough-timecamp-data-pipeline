#!/usr/bin/env python3
import os
import json
import csv
import argparse
import time
import requests
import warnings
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

# Suppress SSL verification warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# === INLINE LOGGER SETUP ===
def setup_logger(name: str = 'timecamp_sync', debug: bool = False) -> logging.Logger:
    """Set up and return a logger instance."""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        os.makedirs('logs', exist_ok=True)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler = RotatingFileHandler(
            'logs/sync.log',
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
    else:
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler):
                handler.setLevel(logging.DEBUG if debug else logging.INFO)
    
    return logger

# === INLINE CONFIG ===
@dataclass
class TimeCampConfig:
   api_key: str
   domain: str = 'app.timecamp.com'
   root_group_id: int = 0
   ignored_user_ids: List[int] = None
   use_supervisor_groups: bool = False
   skip_departments: Optional[str] = None

   def __post_init__(self):
       if self.ignored_user_ids is None:
           self.ignored_user_ids = []

   @classmethod
   def from_env_or_json(cls, json_file_path="TIMECAMP_API_TOKEN.json"):
       """Load config from environment variable first, then fall back to JSON file"""
       # Check for environment variable first (for Cloud Run)
       api_key = os.getenv('TIMECAMP_API_TOKEN')
       if api_key:
           return cls(
               api_key=api_key,
               domain="app.timecamp.com",
               root_group_id=0,
               ignored_user_ids=[],
               use_supervisor_groups=False,
               skip_departments=None
           )
       
       # Fall back to JSON file (for local development)
       return cls.from_json(json_file_path)

   @classmethod
   def from_json(cls, json_file_path="TIMECAMP_API_TOKEN.json"):
       """Load config from JSON file for local testing"""
       try:
           with open(json_file_path, 'r') as f:
               json_data = json.load(f)
               api_key = json_data.get('TIME_CAMP_API_TOKEN')
               
               if not api_key:
                   raise ValueError(f"TIME_CAMP_API_TOKEN not found in {json_file_path}")
               
               return cls(
                   api_key=api_key,
                   domain="app.timecamp.com",
                   root_group_id=0,
                   ignored_user_ids=[],
                   use_supervisor_groups=False,
                   skip_departments=None
               )
       except FileNotFoundError:
           raise FileNotFoundError(f"Could not find {json_file_path}. Please ensure the file exists.")
       except json.JSONDecodeError:
           raise ValueError(f"Invalid JSON format in {json_file_path}")

# === INLINE UTILS ===
def get_yesterday():
    """Get yesterday's date in YYYY-MM-DD format"""
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def parse_date(date_str):
    """Parse date string to YYYY-MM-DD format"""
    if date_str.lower() == 'yesterday':
        return get_yesterday()
    
    try:
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%m-%d-%Y']:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        raise ValueError(f"Could not parse date: {date_str}")
    except Exception as e:
        raise ValueError(f"Invalid date format: {date_str}. Error: {str(e)}")

# === INLINE API CLASS ===
class TimeCampAPI:
    def __init__(self, config: TimeCampConfig):
        self.base_url = f"https://{config.domain}/third_party/api"
        self.headers = {"Accept": "application/json", "Content-Type": "application/json", "Authorization": f"Bearer {config.api_key}"}

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        max_retries, retry_delay = 5, 5
        
        for attempt in range(max_retries):
            try:
                response = requests.request(method, url, headers=self.headers, verify=False, **kwargs)
                
                if response.status_code == 429 and attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if getattr(e.response, 'status_code', None) == 429 and attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                raise
        raise requests.exceptions.RequestException(f"Failed after {max_retries} retries")

    def get_time_entries(self, from_date: str, to_date: str, user_ids: Optional[List[int]] = None, 
                      include_project: bool = True, include_rates: bool = True,
                      opt_fields: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get time entries for specified date range."""
        params = {
            "from": from_date,
            "to": to_date,
            "format": "json",
            "include_project": "1" if include_project else "0",
            "include_rates": "1" if include_rates else "0"
        }
        
        if user_ids:
            params["user_ids"] = ",".join(map(str, user_ids))
            
        if opt_fields:
            params["opt_fields"] = opt_fields
        
        response = self._make_request('GET', "entries", params=params)
        entries = response.json()
        
        return entries

    def get_user_details(self) -> Dict[str, Any]:
        """Get detailed information about all users in the system."""
        response = self._make_request('GET', "people_picker")
        return response.json()

# === GCS FUNCTIONS ===
def setup_gcs_client(credentials_path):
    """Set up Google Cloud Storage client."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return storage.Client(credentials=credentials)

def upload_to_gcs(data, bucket_name, file_name, credentials_path, format_type, logger):
    """Upload data to Google Cloud Storage."""
    try:
        # Set up GCS client
        client = setup_gcs_client(credentials_path)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Prepare data based on format
        if format_type == "csv":
            # Convert to CSV string
            if data:
                fieldnames = set()
                for entry in data:
                    fieldnames.update(entry.keys())
                fieldnames = sorted(list(fieldnames))
                
                # Preferred column order
                preferred_order = [
                    'id', 'date', 'start_time', 'end_time', 'duration', 
                    'user_id', 'user_name', 'email', 'task_id', 'name', 
                    'description', 'task_note', 'project_id', 'project_name',
                    'billable', 'total_cost', 'total_income', 'rate_income',
                    'group_name', 'group_breadcrumb_level_1', 'group_breadcrumb_level_2',
                    'group_breadcrumb_level_3', 'group_breadcrumb_level_4',
                    'locked', 'last_modify', 'color', 'addons_external_id',
                    'invoiceId', 'hasEntryLocationHistory', 'tags', 'breadcrumps'
                ]
                
                ordered_fieldnames = []
                for field in preferred_order:
                    if field in fieldnames:
                        ordered_fieldnames.append(field)
                        fieldnames.remove(field)
                ordered_fieldnames.extend(sorted(fieldnames))
                
                # Create CSV content
                import io
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=ordered_fieldnames)
                writer.writeheader()
                
                for entry in data:
                    flattened_entry = {}
                    for key, value in entry.items():
                        if isinstance(value, (dict, list)):
                            flattened_entry[key] = json.dumps(value, ensure_ascii=False)
                        else:
                            flattened_entry[key] = value
                    
                    row = {field: flattened_entry.get(field, '') for field in ordered_fieldnames}
                    writer.writerow(row)
                
                content = output.getvalue()
                output.close()
            else:
                content = "No data available for the specified date range\n"
                
        elif format_type == "json":
            content = json.dumps(data, indent=2, ensure_ascii=False)
        else:  # jsonl
            content = "\n".join(json.dumps(entry, ensure_ascii=False) for entry in data)
        
        # Upload to GCS
        blob.upload_from_string(content, content_type='text/plain')
        
        logger.info(f"Data uploaded to gs://{bucket_name}/{file_name}")
        logger.info(f"Total records uploaded: {len(data)}")
        
        return f"gs://{bucket_name}/{file_name}"
        
    except Exception as e:
        logger.error(f"Error uploading to GCS: {str(e)}")
        raise

# === EXISTING FUNCTIONS (modified for GCS) ===
def enrich_entries_with_user_details(entries, api, logger):
    """Add user details to each time entry."""
    logger.info("Fetching user details to enrich time entries")

    user_details = api.get_user_details()

    if logger.isEnabledFor(10):  # DEBUG level
        logger.debug(f"User details keys: {', '.join(user_details.keys())}")
        if "users" in user_details:
            logger.debug(f"Number of users: {len(user_details['users'])}")
        if "groups" in user_details:
            logger.debug(f"Number of groups: {len(user_details['groups'])}")

    user_info = {}
    user_id_mapping = {}

    for user_id, user_data in user_details.get("users", {}).items():
        numeric_id = user_id
        if user_id.startswith("u"):
            numeric_id = user_id[1:]
            user_id_mapping[numeric_id] = user_id

        user_info[user_id] = {"email": user_data.get("email", ""), "groups": {}}

        if numeric_id != user_id:
            user_info[numeric_id] = user_info[user_id]

    groups_data = user_details.get("groups", {})
    group_breadcrumbs = {}

    def get_breadcrumb_path(group_id, level=0):
        if group_id not in groups_data:
            if group_id.startswith("g"):
                group_id_no_prefix = group_id[1:]
                if f"g{group_id_no_prefix}" in groups_data:
                    group_id = f"g{group_id_no_prefix}"
                else:
                    return []
            else:
                if f"g{group_id}" in groups_data:
                    group_id = f"g{group_id}"
                else:
                    return []

        group = groups_data[group_id]
        name = group.get("name", "")
        parent_id = group.get("parent_id")

        if not parent_id or parent_id == "0":
            return [name]

        parent_path = get_breadcrumb_path(
            f"g{parent_id}" if not parent_id.startswith("g") else parent_id, level + 1
        )
        return parent_path + [name]

    for group_id, group_data in groups_data.items():
        breadcrumb_path = get_breadcrumb_path(group_id)
        group_breadcrumbs[group_id] = breadcrumb_path

        users = group_data.get("users", {})

        if isinstance(users, dict):
            for user_id in users.keys():
                if user_id in user_info:
                    user_info[user_id]["groups"][group_id] = {
                        "group_name": group_data.get("name", ""),
                        "breadcrumb_path": breadcrumb_path,
                    }

    for entry in entries:
        user_id = entry.get("user_id")
        if user_id and user_id in user_info:
            entry["email"] = user_info[user_id]["email"]

            user_groups = user_info[user_id]["groups"]
            if user_groups:
                first_group_id = next(iter(user_groups))
                group_data = user_groups[first_group_id]

                entry["group_name"] = group_data["group_name"]

                breadcrumb_path = group_data["breadcrumb_path"]
                for i in range(4):
                    if i < len(breadcrumb_path):
                        entry[f"group_breadcrumb_level_{i+1}"] = breadcrumb_path[i]
                    else:
                        entry[f"group_breadcrumb_level_{i+1}"] = ""
        else:
            entry["email"] = ""
            entry["group_name"] = ""
            for i in range(1, 5):
                entry[f"group_breadcrumb_level_{i}"] = ""

    logger.info("Time entries enriched with user details")
    return entries

def get_entries_with_last_modified_filter(api, from_date, to_date, modified_since_hours=24, logger=None):
    """Fetch entries but prioritize recently modified ones to reduce API calls."""
    if logger:
        logger.info(f"Fetching entries from {from_date} to {to_date}")
    
    yesterday = get_yesterday()
    
    if from_date == yesterday or to_date == yesterday:
        return api.get_time_entries(
            from_date, to_date,
            include_project=True,
            include_rates=True,
            opt_fields="tags,breadcrumps"
        )
    else:
        entries = api.get_time_entries(
            from_date, to_date,
            include_project=True,
            include_rates=True,
            opt_fields="tags,breadcrumps"
        )
        
        if modified_since_hours and entries:
            cutoff_time = datetime.now() - timedelta(hours=modified_since_hours)
            cutoff_timestamp = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')
            
            filtered_entries = []
            for entry in entries:
                last_modify = entry.get('last_modify', '')
                if last_modify and last_modify >= cutoff_timestamp:
                    filtered_entries.append(entry)
            
            if logger:
                logger.info(f"Filtered {len(entries)} entries to {len(filtered_entries)} recently modified entries")
            
            return filtered_entries
        
        return entries

def fetch_incremental_data(api, days_back=7, logger=None):
    """Fetch data incrementally with smart rate limiting."""
    all_entries = []
    
    yesterday = get_yesterday()
    logger.info("=== Fetching Yesterday's Data (Full) ===")
    yesterday_entries = get_entries_with_last_modified_filter(
        api, yesterday, yesterday, modified_since_hours=None, logger=logger
    )
    all_entries.extend(yesterday_entries)
    logger.info(f"Yesterday: {len(yesterday_entries)} entries")
    
    for i in range(2, days_back + 1):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        logger.info(f"=== Fetching Day -{i} ({date}) - Modified in Last 48h ===")
        
        try:
            modified_entries = get_entries_with_last_modified_filter(
                api, date, date, modified_since_hours=48, logger=logger
            )
            all_entries.extend(modified_entries)
            logger.info(f"Day -{i}: {len(modified_entries)} modified entries")
            
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error fetching day -{i} ({date}): {str(e)}")
            continue
    
    return all_entries

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch TimeCamp time entries and upload to Google Cloud Storage",
        epilog="Fetches yesterday's data fully, plus modified entries from last 7 days.",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="Number of days back to check for modifications. Default: 7",
    )
    parser.add_argument(
        "--bucket", 
        default="cia_good_enough_timecamp_data", 
        help="GCS bucket name"
    )
    parser.add_argument(
        "--file-name",
        default=None,
        help="Output file name in GCS (auto-generated if not provided)"
    )
    parser.add_argument(
        "--format",
        choices=["json", "jsonl", "csv"],
        default="jsonl",
        help="Output format. Default: jsonl",
    )
    parser.add_argument(
        "--credentials",
        default="/service-account/dbt-mark_cia-data-6be89f0747c2.json",
        help="Path to service account JSON file"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    return parser.parse_args()

def main():
    """Main function for incremental data fetching and GCS upload."""
    args = parse_arguments()
    
    # Set up logger
    logger = setup_logger("timecamp_incremental", args.debug)
    
    try:
        logger.info("Starting TimeCamp incremental data fetch...")
        logger.info(f"Fetching last {args.days_back} days")
        logger.info(f"Target bucket: gs://{args.bucket}")
        
        # Validate service account file
        if not os.path.exists(args.credentials):
            logger.error(f"Service account file not found: {args.credentials}")
            exit(1)
        
        # Initialize API client
        config = TimeCampConfig.from_env_or_json()
        api = TimeCampAPI(config)
        
        # Fetch incremental data
        entries = fetch_incremental_data(api, args.days_back, logger)
        
        if not entries:
            logger.warning("No entries found")
            return
        
        # Enrich with user details
        enriched_entries = enrich_entries_with_user_details(entries, api, logger)
        
        # Remove duplicates based on entry ID
        seen_ids = set()
        unique_entries = []
        for entry in enriched_entries:
            entry_id = entry.get('id')
            if entry_id not in seen_ids:
                unique_entries.append(entry)
                seen_ids.add(entry_id)
        
        logger.info(f"Removed {len(enriched_entries) - len(unique_entries)} duplicate entries")
        
        # Generate file name if not provided
        if not args.file_name:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            args.file_name = f"timecamp_incremental_data_{timestamp}.{args.format}"
        
        # Upload to GCS
        gcs_path = upload_to_gcs(
            unique_entries, 
            args.bucket, 
            args.file_name, 
            args.credentials,
            args.format,
            logger
        )
        
        logger.info("=== Summary ===")
        logger.info(f"Total unique entries: {len(unique_entries)}")
        logger.info(f"Date range covered: {args.days_back} days back")
        logger.info(f"Uploaded to: {gcs_path}")
        logger.info("Incremental fetch and upload completed successfully!")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.error("Failed to fetch incremental data", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main()