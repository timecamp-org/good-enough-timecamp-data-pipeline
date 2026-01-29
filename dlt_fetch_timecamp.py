#!/usr/bin/env python3
"""
DLT pipeline to fetch TimeCamp data and save to files.

Usage:
    python dlt_fetch_timecamp.py --from 2024-01-01 --to 2024-01-31
    python dlt_fetch_timecamp.py --from yesterday --to yesterday
    python dlt_fetch_timecamp.py --datasets entries,users
    python dlt_fetch_timecamp.py --datasets entries,tasks,computer_activities,users
    python dlt_fetch_timecamp.py --format parquet
    python dlt_fetch_timecamp.py --output ./output --debug
"""
import os
import argparse
from typing import Iterator, Dict, Any, List
from datetime import datetime, timedelta

import dlt
from dotenv import load_dotenv

from common.logger import setup_logger
from common.utils import TimeCampConfig, parse_date, get_yesterday
from common.api import TimeCampAPI


# Supported output formats for dlt filesystem destination
SUPPORTED_FORMATS = ["csv", "jsonl", "parquet"]

# Available datasets
AVAILABLE_DATASETS = ["entries", "tasks", "computer_activities", "users"]


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch TimeCamp data using DLT and save to files",
        epilog="By default, fetches data for yesterday unless specified otherwise."
    )
    parser.add_argument("--from", dest="from_date", default="yesterday",
                        help="Start date (YYYY-MM-DD format or 'yesterday'). Default: yesterday")
    parser.add_argument("--to", dest="to_date", default="yesterday",
                        help="End date (YYYY-MM-DD format or 'yesterday'). Default: yesterday")
    parser.add_argument("--output", default="./timecamp_data",
                        help="Output directory path. Default: ./timecamp_data")
    parser.add_argument("--format", dest="output_format", 
                        choices=SUPPORTED_FORMATS, default="csv",
                        help="Output format: csv, jsonl, or parquet. Default: csv")
    parser.add_argument("--datasets", default="entries",
                        help=f"Comma-separated list of datasets to fetch. Available: {', '.join(AVAILABLE_DATASETS)}. Default: entries")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    
    return parser.parse_args()


def parse_datasets(datasets_str: str) -> List[str]:
    """Parse and validate the datasets parameter."""
    datasets = [d.strip().lower() for d in datasets_str.split(",")]
    
    invalid = [d for d in datasets if d not in AVAILABLE_DATASETS]
    if invalid:
        raise ValueError(f"Invalid datasets: {', '.join(invalid)}. Available: {', '.join(AVAILABLE_DATASETS)}")
    
    return datasets


def setup_environment(debug: bool = False):
    """Set up the environment and return API client."""
    logger = setup_logger('dlt_timecamp', debug)
    load_dotenv()
    config = TimeCampConfig.from_env()
    api = TimeCampAPI(config, debug)
    return logger, api


def get_date_range(from_date: str, to_date: str) -> List[str]:
    """Generate a list of dates between from_date and to_date (inclusive)."""
    from_dt = datetime.strptime(from_date, "%Y-%m-%d")
    to_dt = datetime.strptime(to_date, "%Y-%m-%d")
    
    dates = []
    current = from_dt
    while current <= to_dt:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    return dates


def get_user_details_lookup(api: TimeCampAPI, logger) -> Dict[str, Dict[str, Any]]:
    """Build a user details lookup dictionary from the API."""
    logger.info("Fetching user details for enrichment")
    user_details = api.get_user_details()
    
    user_info = {}
    groups_data = user_details.get('groups', {})
    
    def get_breadcrumb_path(group_id: str, visited: set = None) -> List[str]:
        if visited is None:
            visited = set()
        
        if group_id in visited:
            return []
        visited.add(group_id)
        
        actual_group_id = group_id
        if group_id not in groups_data:
            if group_id.startswith('g'):
                actual_group_id = group_id
            else:
                actual_group_id = f"g{group_id}"
        
        if actual_group_id not in groups_data:
            return []
        
        group = groups_data[actual_group_id]
        name = group.get('name', '')
        parent_id = group.get('parent_id')
        
        if not parent_id or parent_id == "0":
            return [name]
        
        parent_group_id = f"g{parent_id}" if not parent_id.startswith('g') else parent_id
        parent_path = get_breadcrumb_path(parent_group_id, visited)
        return parent_path + [name]
    
    group_breadcrumbs = {}
    for group_id, group_data in groups_data.items():
        group_breadcrumbs[group_id] = get_breadcrumb_path(group_id)
    
    for user_id, user_data in user_details.get('users', {}).items():
        numeric_id = user_id[1:] if user_id.startswith('u') else user_id
        
        user_entry = {
            'email': user_data.get('email', ''),
            'groups': {}
        }
        
        user_info[user_id] = user_entry
        if numeric_id != user_id:
            user_info[numeric_id] = user_entry
    
    for group_id, group_data in groups_data.items():
        users = group_data.get('users', {})
        if isinstance(users, dict):
            for user_id in users.keys():
                if user_id in user_info:
                    user_info[user_id]['groups'][group_id] = {
                        'group_name': group_data.get('name', ''),
                        'breadcrumb_path': group_breadcrumbs.get(group_id, [])
                    }
    
    logger.info(f"Built user lookup with {len(user_info)} user entries")
    return user_info


def enrich_entry(entry: Dict[str, Any], user_info: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Enrich a single time entry with user details."""
    user_id = entry.get('user_id')
    
    if user_id and str(user_id) in user_info:
        user_data = user_info[str(user_id)]
        entry['email'] = user_data['email']
        
        user_groups = user_data['groups']
        if user_groups:
            first_group_id = next(iter(user_groups))
            group_data = user_groups[first_group_id]
            entry['group_name'] = group_data['group_name']
            
            breadcrumb_path = group_data['breadcrumb_path']
            for i in range(4):
                entry[f'group_breadcrumb_level_{i+1}'] = breadcrumb_path[i] if i < len(breadcrumb_path) else ''
        else:
            entry['group_name'] = ''
            for i in range(1, 5):
                entry[f'group_breadcrumb_level_{i}'] = ''
    else:
        entry['email'] = ''
        entry['group_name'] = ''
        for i in range(1, 5):
            entry[f'group_breadcrumb_level_{i}'] = ''
    
    return entry


@dlt.source(name="timecamp")
def timecamp_source(
    api: TimeCampAPI,
    from_date: str,
    to_date: str,
    datasets: List[str],
    logger,
    enrich_with_users: bool = True
):
    """
    DLT source for TimeCamp data.
    
    Args:
        api: TimeCampAPI instance
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format
        datasets: List of datasets to fetch
        logger: Logger instance
        enrich_with_users: Whether to enrich entries with user details
    """
    resources = []
    
    # Preload user info if needed for enrichment
    user_info = {}
    if enrich_with_users and ("entries" in datasets or "computer_activities" in datasets):
        user_info = get_user_details_lookup(api, logger)
    
    if "entries" in datasets:
        @dlt.resource(
            name="entries",
            write_disposition="replace",
            primary_key="id"
        )
        def entries_resource() -> Iterator[Dict[str, Any]]:
            """Fetch and yield time entries from TimeCamp API."""
            logger.info(f"Fetching time entries from {from_date} to {to_date}")
            
            entries = api.get_time_entries(
                from_date,
                to_date,
                include_project=True,
                include_rates=True,
                opt_fields="tags,breadcrumps"
            )
            
            logger.info(f"Retrieved {len(entries)} time entries")
            
            for entry in entries:
                if enrich_with_users:
                    entry = enrich_entry(entry, user_info)
                yield entry
            
            logger.info(f"Processed {len(entries)} time entries")
        
        resources.append(entries_resource)
    
    if "tasks" in datasets:
        @dlt.resource(
            name="tasks",
            write_disposition="replace",
            primary_key="task_id"
        )
        def tasks_resource() -> Iterator[Dict[str, Any]]:
            """Fetch and yield tasks from TimeCamp API."""
            logger.info("Fetching tasks")
            
            tasks = api.get_tasks()
            logger.info(f"Retrieved {len(tasks)} tasks")
            
            for task in tasks:
                yield task
        
        resources.append(tasks_resource)
    
    if "computer_activities" in datasets:
        @dlt.resource(
            name="computer_activities",
            write_disposition="replace"
        )
        def computer_activities_resource() -> Iterator[Dict[str, Any]]:
            """Fetch and yield computer activities from TimeCamp API."""
            dates = get_date_range(from_date, to_date)
            logger.info(f"Fetching computer activities for {len(dates)} days")
            
            activities = api.get_computer_activities(
                dates=dates,
                include="application,window_title"
            )
            
            logger.info(f"Retrieved {len(activities)} computer activities")
            
            for activity in activities:
                if enrich_with_users:
                    activity = enrich_entry(activity, user_info)
                yield activity
        
        resources.append(computer_activities_resource)
    
    if "users" in datasets:
        @dlt.resource(
            name="users",
            write_disposition="replace",
            primary_key="user_id"
        )
        def users_resource() -> Iterator[Dict[str, Any]]:
            """Fetch and yield users from TimeCamp API."""
            logger.info("Fetching users")
            
            users = api.get_users()
            logger.info(f"Retrieved {len(users)} users")
            
            for user in users:
                yield user
        
        resources.append(users_resource)
    
    return resources


def run_pipeline(
    from_date: str,
    to_date: str,
    output_dir: str,
    output_format: str,
    datasets: List[str],
    logger,
    api: TimeCampAPI
):
    """
    Run the DLT pipeline to fetch TimeCamp data and save to files.
    
    Args:
        from_date: Start date string
        to_date: End date string
        output_dir: Output directory for files
        output_format: Output format (csv, jsonl, parquet)
        datasets: List of datasets to fetch
        logger: Logger instance
        api: TimeCampAPI instance
    """
    from_date_parsed = parse_date(from_date)
    to_date_parsed = parse_date(to_date)
    
    logger.info(f"Starting DLT pipeline: {from_date_parsed} to {to_date_parsed}")
    logger.info(f"Datasets: {', '.join(datasets)}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Output format: {output_format}")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Disable gzip compression for output files
    dlt.config["normalize.data_writer.disable_compression"] = True
    
    pipeline = dlt.pipeline(
        pipeline_name="timecamp",
        destination=dlt.destinations.filesystem(
            bucket_url=output_dir,
            layout="{table_name}.{file_id}.{ext}",
        ),
        dataset_name="timecamp",
    )
    
    source = timecamp_source(
        api=api,
        from_date=from_date_parsed,
        to_date=to_date_parsed,
        datasets=datasets,
        logger=logger,
        enrich_with_users=True
    )
    
    load_info = pipeline.run(
        source,
        loader_file_format=output_format
    )
    
    logger.info(f"Pipeline completed: {load_info}")
    logger.info(f"{output_format.upper()} files saved to: {output_dir}")
    
    return load_info


def main():
    """Main function."""
    args = parse_arguments()
    
    # Parse and validate datasets
    try:
        datasets = parse_datasets(args.datasets)
    except ValueError as e:
        print(f"Error: {e}")
        exit(1)
    
    logger, api = setup_environment(args.debug)
    
    try:
        load_info = run_pipeline(
            from_date=args.from_date,
            to_date=args.to_date,
            output_dir=args.output,
            output_format=args.output_format,
            datasets=datasets,
            logger=logger,
            api=api
        )
        
        print(f"\nPipeline completed successfully!")
        print(f"Datasets: {', '.join(datasets)}")
        print(f"Data saved to: {args.output} ({args.output_format} format)")
        print(f"\nLoad info:\n{load_info}")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
