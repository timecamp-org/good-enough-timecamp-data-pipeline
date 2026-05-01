#!/usr/bin/env python3
"""
DLT pipeline to fetch TimeCamp data and save to files.

Usage:
    python dlt_fetch_timecamp.py --from 2024-01-01 --to 2024-01-31
    python dlt_fetch_timecamp.py --from yesterday --to yesterday
    python dlt_fetch_timecamp.py --datasets entries,users
    python dlt_fetch_timecamp.py --datasets entries,tasks,computer_activities,users,application_names
    python dlt_fetch_timecamp.py --format parquet
    python dlt_fetch_timecamp.py --output ./output --debug

Available datasets:
    - entries: Time entries with project/task details
    - tasks: Task hierarchy with breadcrumb paths
    - computer_activities: Desktop app tracking data
    - users: User details with group information
    - application_names: Application lookup table with names and categories
"""

import argparse
from calendar import monthrange
import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Tuple

import dlt
from dotenv import load_dotenv

from common.api import TimeCampAPI
from common.logger import setup_logger
from common.utils import TimeCampConfig, get_yesterday, parse_date

# Supported output formats for dlt filesystem destination
SUPPORTED_FORMATS = ["csv", "jsonl", "parquet"]
ENTRY_BATCH_MONTHS = 6

# Available datasets
AVAILABLE_DATASETS = [
    "entries",
    "tasks",
    "computer_activities",
    "users",
    "application_names",
]


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch TimeCamp data using DLT and save to files",
        epilog="By default, fetches data for yesterday unless specified otherwise.",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        default="yesterday",
        help="Start date (YYYY-MM-DD format or 'yesterday'). Default: yesterday",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        default="yesterday",
        help="End date (YYYY-MM-DD format or 'yesterday'). Default: yesterday",
    )
    parser.add_argument(
        "--output",
        default="./timecamp_data",
        help="Output directory path. Default: ./timecamp_data",
    )
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=SUPPORTED_FORMATS,
        default="csv",
        help="Output format: csv, jsonl, or parquet. Default: csv",
    )
    parser.add_argument(
        "--datasets",
        default="entries",
        help=f"Comma-separated list of datasets to fetch. Available: {', '.join(AVAILABLE_DATASETS)}. Default: entries",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    return parser.parse_args()


def parse_datasets(datasets_str: str) -> List[str]:
    """Parse and validate the datasets parameter."""
    datasets = [d.strip().lower() for d in datasets_str.split(",")]

    invalid = [d for d in datasets if d not in AVAILABLE_DATASETS]
    if invalid:
        raise ValueError(
            f"Invalid datasets: {', '.join(invalid)}. Available: {', '.join(AVAILABLE_DATASETS)}"
        )

    return datasets


def setup_environment(debug: bool = False):
    """Set up the environment and return API client."""
    logger = setup_logger("dlt_timecamp", debug)
    load_dotenv(override=True)
    config = TimeCampConfig.from_env()
    api = TimeCampAPI(config, debug)
    return logger, api


def get_category_mapping() -> Dict[str, str]:
    """Return the category ID to name mapping."""
    return {
        "0": "No category",
        "1": "Office",
        "2": "Developer Tools",
        "3": "Chat, VoIP & Email",
        "4": "Graphic & Design",
        "5": "Home",
        "6": "Productivity",
        "7": "Utilities & Tools",
        "8": "Audio & Video",
        "9": "Games",
        "10": "Education",
        "11": "Fun",
        "12": "News & Blogs",
        "13": "Reference & Search",
        "14": "Shopping",
        "15": "Social Networking",
        "16": "Travel & Outdoors",
        "17": "Business",
        "18": "Hobby",
    }


def get_application_name_fallback(app_details: Dict[str, Any]) -> str:
    """Get application name using fallback logic: full_name -> additional_info -> app_name."""
    full_name = app_details.get("full_name", "") or ""
    additional_info = app_details.get("aditional_info", "") or ""  # Note: API typo
    app_name = app_details.get("app_name", "") or ""

    # Use first non-empty value
    if full_name.strip():
        return full_name.strip()
    elif additional_info.strip():
        return additional_info.strip()
    else:
        return app_name.strip()


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


def add_months(date_value: datetime, months: int) -> datetime:
    """Add calendar months while keeping the day valid for the target month."""
    month_index = date_value.month - 1 + months
    year = date_value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(date_value.day, monthrange(year, month)[1])
    return date_value.replace(year=year, month=month, day=day)


def get_date_period_batches(
    from_date: str, to_date: str, months: int = ENTRY_BATCH_MONTHS
) -> List[Tuple[str, str]]:
    """Split an inclusive date period into calendar-month batches."""
    from_dt = datetime.strptime(from_date, "%Y-%m-%d")
    to_dt = datetime.strptime(to_date, "%Y-%m-%d")

    batches = []
    current = from_dt
    while current <= to_dt:
        next_start = add_months(current, months)
        batch_end = min(next_start - timedelta(days=1), to_dt)
        batches.append(
            (current.strftime("%Y-%m-%d"), batch_end.strftime("%Y-%m-%d"))
        )
        current = batch_end + timedelta(days=1)

    return batches


ACTIVITIES_CACHE_FILE = "computer_activities_cache.json"
CACHE_THRESHOLD_DAYS = 7


def classify_dates(
    dates: List[str], threshold_days: int = CACHE_THRESHOLD_DAYS
) -> tuple:
    """Split dates into old (cacheable) and recent (always fetch fresh).

    Returns:
        (old_dates, recent_dates) tuple of date string lists
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = today - timedelta(days=threshold_days)

    old_dates = []
    recent_dates = []
    for d in dates:
        if datetime.strptime(d, "%Y-%m-%d") < cutoff:
            old_dates.append(d)
        else:
            recent_dates.append(d)

    return old_dates, recent_dates


def load_activities_cache() -> Dict[str, list]:
    """Load computer activities cache from file. Keyed by date string."""
    if not os.path.exists(ACTIVITIES_CACHE_FILE):
        return {}
    try:
        with open(ACTIVITIES_CACHE_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_activities_cache(cache: Dict[str, list]) -> None:
    """Save computer activities cache to file."""
    try:
        with open(ACTIVITIES_CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except IOError:
        pass


def preload_computer_activities(
    api: TimeCampAPI, from_date: str, to_date: str, user_ids: List[int], logger
) -> tuple:
    """Fetch computer activities with caching. Dates older than CACHE_THRESHOLD_DAYS
    are served from a local JSON cache; recent dates are always fetched fresh.

    Returns:
        (activities, application_ids) — list of activity dicts and set of app ID strings
    """
    dates = get_date_range(from_date, to_date)
    old_dates, recent_dates = classify_dates(dates)
    logger.info(
        f"Preloading computer activities: {len(dates)} days "
        f"({len(old_dates)} cacheable, {len(recent_dates)} recent)"
    )

    cache = load_activities_cache()
    activities = []

    if old_dates:
        cached_dates = [d for d in old_dates if d in cache]
        missing_dates = [d for d in old_dates if d not in cache]

        if cached_dates:
            logger.info(f"Cache hit for {len(cached_dates)} old dates")
            for d in cached_dates:
                activities.extend(cache[d])

        if missing_dates:
            logger.info(
                f"Cache miss for {len(missing_dates)} old dates, fetching from API"
            )
            fetched = api.get_computer_activities(
                dates=missing_dates,
                include="application,window_title",
                user_ids=user_ids,
            )
            for activity in fetched:
                d = activity.get("end_date", "")
                if d:
                    cache.setdefault(d, []).append(activity)
            activities.extend(fetched)

    if recent_dates:
        logger.info(f"Fetching {len(recent_dates)} recent dates from API")
        fetched = api.get_computer_activities(
            dates=recent_dates,
            include="application,window_title",
            user_ids=user_ids,
        )
        activities.extend(fetched)

    save_activities_cache(cache)
    logger.info(f"Preloaded {len(activities)} computer activities total")

    application_ids = {
        str(a["application_id"])
        for a in activities
        if a.get("application_id") and str(a["application_id"]) != "0"
    }
    logger.info(f"Found {len(application_ids)} unique application IDs")

    return activities, application_ids


def get_user_details_lookup(api: TimeCampAPI, logger) -> Dict[str, Dict[str, Any]]:
    """Build a user details lookup dictionary from the API."""
    logger.info("Fetching user details for enrichment")
    user_details = api.get_user_details()

    user_info = {}
    groups_data = user_details.get("groups", {})

    def get_breadcrumb_path(group_id: str, visited: set = None) -> List[str]:
        if visited is None:
            visited = set()

        if group_id in visited:
            return []
        visited.add(group_id)

        actual_group_id = group_id
        if group_id not in groups_data:
            if group_id.startswith("g"):
                actual_group_id = group_id
            else:
                actual_group_id = f"g{group_id}"

        if actual_group_id not in groups_data:
            return []

        group = groups_data[actual_group_id]
        name = group.get("name", "")
        parent_id = group.get("parent_id")

        if not parent_id or parent_id == "0":
            return [name]

        parent_group_id = (
            f"g{parent_id}" if not parent_id.startswith("g") else parent_id
        )
        parent_path = get_breadcrumb_path(parent_group_id, visited)
        return parent_path + [name]

    group_breadcrumbs = {}
    for group_id, group_data in groups_data.items():
        group_breadcrumbs[group_id] = get_breadcrumb_path(group_id)

    for user_id, user_data in user_details.get("users", {}).items():
        numeric_id = user_id[1:] if user_id.startswith("u") else user_id

        user_entry = {"email": user_data.get("email", ""), "groups": {}}

        user_info[user_id] = user_entry
        if numeric_id != user_id:
            user_info[numeric_id] = user_entry

    for group_id, group_data in groups_data.items():
        users = group_data.get("users", {})
        if isinstance(users, dict):
            for user_id in users.keys():
                if user_id in user_info:
                    user_info[user_id]["groups"][group_id] = {
                        "group_name": group_data.get("name", ""),
                        "breadcrumb_path": group_breadcrumbs.get(group_id, []),
                    }

    logger.info(f"Built user lookup with {len(user_info)} user entries")
    return user_info


def enrich_user_with_group(
    user: Dict[str, Any], user_info: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """Enrich a user record with group details."""
    user_id = user.get("user_id")

    # Initialize default empty values
    user["group_name"] = ""
    user["group_breadcrumb"] = ""
    for i in range(1, 6):
        user[f"group_level_{i}"] = ""

    if user_id and str(user_id) in user_info:
        user_data = user_info[str(user_id)]
        user_groups = user_data.get("groups", {})

        if user_groups:
            # Get the first group (primary group)
            first_group_id = next(iter(user_groups))
            group_data = user_groups[first_group_id]

            user["group_name"] = group_data.get("group_name", "")

            breadcrumb_path = group_data.get("breadcrumb_path", [])
            # Create breadcrumb string separated by /
            user["group_breadcrumb"] = (
                " / ".join(breadcrumb_path) if breadcrumb_path else ""
            )

            # Populate group_level_1 through group_level_5
            for i in range(5):
                user[f"group_level_{i + 1}"] = (
                    breadcrumb_path[i] if i < len(breadcrumb_path) else ""
                )

    return user


@dlt.source(name="timecamp")
def timecamp_source(
    api: TimeCampAPI,
    from_date: str,
    to_date: str,
    datasets: List[str],
    logger,
    enrich_with_users: bool = True,
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
    if enrich_with_users and "users" in datasets:
        user_info = get_user_details_lookup(api, logger)

    # Preload active user IDs for computer_activities and application_names
    # TimeCamp API returns empty results for activity endpoint without user_id
    active_user_ids = None
    if "computer_activities" in datasets or "application_names" in datasets:
        logger.info("Fetching active user IDs for computer activities")
        all_users = api.get_users()
        active_user_ids = [
            int(u["user_id"]) for u in all_users if u.get("is_enabled", True)
        ]
        logger.info(f"Found {len(active_user_ids)} active users")

    # Preload computer activities for both computer_activities and application_names
    # DLT extracts resources in round-robin, so we preload eagerly to share data
    preloaded_activities = []
    preloaded_application_ids = set()
    if "computer_activities" in datasets or "application_names" in datasets:
        preloaded_activities, preloaded_application_ids = preload_computer_activities(
            api, from_date, to_date, active_user_ids, logger
        )

    if "entries" in datasets:

        @dlt.resource(name="entries", write_disposition="replace", primary_key="id")
        def entries_resource() -> Iterator[Dict[str, Any]]:
            """Fetch and yield time entries from TimeCamp API."""
            logger.info(f"Fetching time entries from {from_date} to {to_date}")

            date_batches = get_date_period_batches(from_date, to_date)
            if len(date_batches) > 1:
                logger.info(
                    f"Time entries period is longer than {ENTRY_BATCH_MONTHS} months, "
                    f"batching into {len(date_batches)} requests"
                )

            total_entries = 0
            for batch_index, (batch_from, batch_to) in enumerate(date_batches, start=1):
                if len(date_batches) > 1:
                    logger.info(
                        f"Fetching time entries batch {batch_index}/{len(date_batches)}: "
                        f"{batch_from} to {batch_to}"
                    )

                entries = api.get_time_entries(
                    batch_from,
                    batch_to,
                    include_project=True,
                    include_rates=True,
                    opt_fields="tags,breadcrumps",
                )

                total_entries += len(entries)
                if len(date_batches) > 1:
                    logger.info(
                        f"Retrieved {len(entries)} time entries for batch {batch_index}"
                    )
                else:
                    logger.info(f"Retrieved {len(entries)} time entries")

                for entry in entries:
                    # Convert tags to JSON string to avoid separate entries__tags table
                    if "tags" in entry and entry["tags"]:
                        entry["tags"] = json.dumps(entry["tags"])
                    else:
                        entry["tags"] = None
                    yield entry

            logger.info(f"Processed {total_entries} time entries")

        resources.append(entries_resource)

    if "tasks" in datasets:

        @dlt.resource(
            name="tasks",
            write_disposition="replace",
            primary_key="task_id",
            columns={
                "public_hash": {"data_type": "text"},
                "task_key": {"data_type": "text"},
            },
        )
        def tasks_resource() -> Iterator[Dict[str, Any]]:
            """Fetch and yield tasks from TimeCamp API."""
            logger.info("Fetching tasks")

            tasks = api.get_tasks()
            logger.info(f"Retrieved {len(tasks)} tasks")

            # Build task lookup for breadcrumb computation
            task_lookup = {str(t.get("task_id")): t for t in tasks}

            def get_task_breadcrumb_path(
                task_id: str, visited: set = None
            ) -> List[str]:
                """Recursively build the breadcrumb path for a task."""
                if visited is None:
                    visited = set()

                if task_id in visited or task_id not in task_lookup:
                    return []
                visited.add(task_id)

                task = task_lookup[task_id]
                name = task.get("name", "")
                parent_id = str(task.get("parent_id", "0"))

                # Root task (no parent or parent_id is 0)
                if not parent_id or parent_id == "0":
                    return [name]

                parent_path = get_task_breadcrumb_path(parent_id, visited)
                return parent_path + [name]

            for task in tasks:
                # Remove users and perms fields to avoid flattened columns and perms table
                task.pop("users", None)
                task.pop("perms", None)

                # Compute task breadcrumb
                task_id = str(task.get("task_id", ""))
                breadcrumb_path = get_task_breadcrumb_path(task_id)

                # Add task_breadcrumb as full path
                task["task_breadcrumb"] = (
                    " / ".join(breadcrumb_path) if breadcrumb_path else ""
                )

                # Add task_level_1 through task_level_8
                for i in range(8):
                    task[f"task_level_{i + 1}"] = (
                        breadcrumb_path[i] if i < len(breadcrumb_path) else ""
                    )

                yield task

        resources.append(tasks_resource)

    if "computer_activities" in datasets:

        @dlt.resource(name="computer_activities", write_disposition="replace")
        def computer_activities_resource() -> Iterator[Dict[str, Any]]:
            """Yield preloaded computer activities."""
            logger.info(f"Yielding {len(preloaded_activities)} computer activities")
            for activity in preloaded_activities:
                yield activity

        resources.append(computer_activities_resource)

    if "users" in datasets:

        @dlt.resource(name="users", write_disposition="replace", primary_key="user_id")
        def users_resource() -> Iterator[Dict[str, Any]]:
            """Fetch and yield users from TimeCamp API."""
            logger.info("Fetching users")

            users = api.get_users()
            logger.info(f"Retrieved {len(users)} users")

            for user in users:
                if enrich_with_users:
                    user = enrich_user_with_group(user, user_info)
                yield user

        resources.append(users_resource)

    if "application_names" in datasets:

        @dlt.resource(
            name="application_names",
            write_disposition="replace",
            primary_key="application_id",
        )
        def application_names_resource() -> Iterator[Dict[str, Any]]:
            """Fetch and yield application names from TimeCamp API.

            Uses preloaded_application_ids collected during activity preloading.
            """
            logger.info(
                f"Fetching application names for {len(preloaded_application_ids)} application IDs"
            )

            if not preloaded_application_ids:
                logger.info("No application IDs found in activities")
                return

            # Fetch application details using cache
            applications = api.get_applications_with_cache(
                list(preloaded_application_ids), batch_size=200
            )

            # Get category mapping for enrichment
            category_mapping = get_category_mapping()

            for app_id, app_details in applications.items():
                # Use fallback logic for application name
                application_name = get_application_name_fallback(app_details)

                # Map category ID to category name
                category_id = str(app_details.get("category_id", "0"))
                category_name = category_mapping.get(category_id, "No category")

                yield {
                    "application_id": app_id,
                    "application_name": application_name,
                    "app_name": app_details.get("app_name", ""),
                    "full_name": app_details.get("full_name", ""),
                    "additional_info": app_details.get(
                        "aditional_info", ""
                    ),  # Note: API typo
                    "category_id": category_id,
                    "category_name": category_name,
                    "type": app_details.get("type", ""),
                    "icon_url": app_details.get("icon_url", ""),
                }

            logger.info(f"Processed {len(applications)} applications")

        resources.append(application_names_resource)

    return resources


def run_pipeline(
    from_date: str,
    to_date: str,
    output_dir: str,
    output_format: str,
    datasets: List[str],
    logger,
    api: TimeCampAPI,
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
        enrich_with_users=True,
    )

    load_info = pipeline.run(source, loader_file_format=output_format)

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
            api=api,
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
