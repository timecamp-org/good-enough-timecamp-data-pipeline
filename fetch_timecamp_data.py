#!/usr/bin/env python3
import os
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
from common.logger import setup_logger
from common.utils import TimeCampConfig, parse_date, get_yesterday
from common.api import TimeCampAPI


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch TimeCamp time entries and save as JSON",
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
        "--output", default=None, help="Output file path. Default: timecamp_data.jsonl"
    )
    parser.add_argument(
        "--format",
        choices=["json", "jsonl"],
        default="jsonl",
        help="Output format: json (pretty) or jsonl (newline-delimited). Default: jsonl",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    return parser.parse_args()


def setup_environment(debug=False):
    """Set up the environment and return API client."""
    # Set up logger
    logger = setup_logger("timecamp_data", debug)

    # Load environment variables
    load_dotenv()

    # Initialize TimeCamp API client
    config = TimeCampConfig.from_env()
    timecamp_api = TimeCampAPI(config)

    return logger, timecamp_api


def fetch_time_entries(api, from_date, to_date, logger):
    """Fetch time entries from TimeCamp."""
    # Parse dates if necessary
    from_date_parsed = parse_date(from_date)
    to_date_parsed = parse_date(to_date)

    logger.info(
        f"Fetching TimeCamp time entries from {from_date_parsed} to {to_date_parsed}"
    )
    logger.info(
        "Including project data, rates, and additional fields (tags, breadcrumps) by default"
    )

    # Fetch time entries from the API with all additional information included
    entries = api.get_time_entries(
        from_date_parsed,
        to_date_parsed,
        include_project=True,
        include_rates=True,
        opt_fields="tags,breadcrumps",
    )

    logger.info(f"Retrieved {len(entries)} time entries")
    return entries


def enrich_entries_with_user_details(entries, api, logger):
    """Add user details to each time entry."""
    logger.info("Fetching user details to enrich time entries")

    # Get user details from API
    user_details = api.get_user_details()

    # Log some debug information about the structure
    if logger.isEnabledFor(10):  # DEBUG level
        logger.debug(f"User details keys: {', '.join(user_details.keys())}")
        if "users" in user_details:
            logger.debug(f"Number of users: {len(user_details['users'])}")
            logger.debug(
                f"First few user IDs: {list(user_details['users'].keys())[:5]}"
            )
        if "groups" in user_details:
            logger.debug(f"Number of groups: {len(user_details['groups'])}")
            logger.debug(
                f"First few group IDs: {list(user_details['groups'].keys())[:5]}"
            )

    # Create user info lookup dictionary
    user_info = {}

    # Create mapping between numeric user IDs and prefixed user IDs
    user_id_mapping = {}

    # Extract and organize user information
    for user_id, user_data in user_details.get("users", {}).items():
        # Store both with and without 'u' prefix for matching
        numeric_id = user_id
        if user_id.startswith("u"):
            numeric_id = user_id[1:]  # Remove 'u' prefix
            user_id_mapping[numeric_id] = user_id

        user_info[user_id] = {"email": user_data.get("email", ""), "groups": {}}

        # Also store with numeric ID for direct matching with time entries
        if numeric_id != user_id:
            user_info[numeric_id] = user_info[user_id]

    # Extract group information and build breadcrumbs
    groups_data = user_details.get("groups", {})
    group_breadcrumbs = {}

    # Function to recursively get breadcrumb path
    def get_breadcrumb_path(group_id, level=0):
        if group_id not in groups_data:
            # Try without 'g' prefix if has it
            if group_id.startswith("g"):
                group_id_no_prefix = group_id[1:]
                if f"g{group_id_no_prefix}" in groups_data:
                    group_id = f"g{group_id_no_prefix}"
                else:
                    return []
            else:
                # Try with 'g' prefix if doesn't have it
                if f"g{group_id}" in groups_data:
                    group_id = f"g{group_id}"
                else:
                    return []

        group = groups_data[group_id]
        name = group.get("name", "")

        # Get parent_id from the group data
        parent_id = group.get("parent_id")

        # Check if this is a root group (parent_id is "0" or none)
        if not parent_id or parent_id == "0":
            return [name]

        # Get the parent path recursively, then append current group name
        parent_path = get_breadcrumb_path(
            f"g{parent_id}" if not parent_id.startswith("g") else parent_id, level + 1
        )
        return parent_path + [name]

    # Build group breadcrumbs for all groups
    for group_id, group_data in groups_data.items():
        breadcrumb_path = get_breadcrumb_path(group_id)
        group_breadcrumbs[group_id] = breadcrumb_path

        # For each user in this group, add the group info
        users = group_data.get("users", {})

        # Handle different format of users (dict vs list)
        if isinstance(users, dict):
            for user_id in users.keys():
                if user_id in user_info:
                    user_info[user_id]["groups"][group_id] = {
                        "group_name": group_data.get("name", ""),
                        "breadcrumb_path": breadcrumb_path,
                    }
        # If users is a list (empty or otherwise), skip processing

    # Enrich entries with user information
    for entry in entries:
        user_id = entry.get("user_id")
        if user_id and user_id in user_info:
            entry["email"] = user_info[user_id]["email"]

            # Get group information for the user
            user_groups = user_info[user_id]["groups"]
            if user_groups:
                # Use the first group for now (most users have only one primary group)
                first_group_id = next(iter(user_groups))
                group_data = user_groups[first_group_id]

                entry["group_name"] = group_data["group_name"]

                # Add breadcrumb levels
                breadcrumb_path = group_data["breadcrumb_path"]
                for i in range(4):
                    if i < len(breadcrumb_path):
                        entry[f"group_breadcrumb_level_{i+1}"] = breadcrumb_path[i]
                    else:
                        entry[f"group_breadcrumb_level_{i+1}"] = ""
        else:
            # Default values if user not found
            entry["email"] = ""
            entry["group_name"] = ""
            for i in range(1, 5):
                entry[f"group_breadcrumb_level_{i}"] = ""

    logger.info("Time entries enriched with user details")
    return entries


def save_to_file(entries, output_path, format_type, logger):
    """Save time entries to a file.

    Args:
        entries: List of time entry dictionaries
        output_path: Path to save the file
        format_type: 'json' for pretty JSON, 'jsonl' for newline-delimited JSON
        logger: Logger object
    """
    # Make sure the directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w") as f:
        if format_type == "json":
            # Pretty JSON format
            json.dump(entries, f, indent=2)
        else:  # jsonl format
            # Newline-delimited JSON format (one JSON object per line)
            for entry in entries:
                f.write(json.dumps(entry) + "\n")

    logger.info(f"Time entries saved to {output_path} in {format_type} format")


def main():
    """Main function."""
    # Parse command-line arguments
    args = parse_arguments()

    # Set up environment
    logger, api = setup_environment(args.debug)

    try:
        # Fetch time entries with additional parameters
        entries = fetch_time_entries(api, args.from_date, args.to_date, logger)

        # Enrich entries with user details
        enriched_entries = enrich_entries_with_user_details(entries, api, logger)

        # Generate default output filename if not specified
        if args.output is None:
            # Use appropriate extension based on format
            extension = ".json" if args.format == "json" else ".jsonl"
            args.output = f"timecamp_data{extension}"

        # Save to file in specified format
        save_to_file(enriched_entries, args.output, args.format, logger)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
