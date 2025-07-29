#!/usr/bin/env python3
import os
import json
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from common.logger import setup_logger
from common.utils import TimeCampConfig, parse_date, get_yesterday
from common.api import TimeCampAPI

def get_category_mapping():
    """Return the category ID to name mapping."""
    return {
        '0': 'No category',
        '1': 'Office',
        '2': 'Developer Tools',
        '3': 'Chat, VoIP & Email',
        '4': 'Graphic & Design',
        '5': 'Home',
        '6': 'Productivity',
        '7': 'Utilities & Tools',
        '8': 'Audio & Video',
        '9': 'Games',
        '10': 'Education',
        '11': 'Fun',
        '12': 'News & Blogs',
        '13': 'Reference & Search',
        '14': 'Shopping',
        '15': 'Social Networking',
        '16': 'Travel & Outdoors',
        '17': 'Business',
        '18': 'Hobby'
    }

def calculate_start_time(end_time_str, time_span_seconds):
    """Calculate start_time by subtracting time_span from end_time."""
    try:
        end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
        start_time = end_time - timedelta(seconds=time_span_seconds)
        return start_time.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return ''

def get_application_name_fallback(app_details):
    """Get application name using fallback logic: full_name -> additional_info -> app_name."""
    full_name = app_details.get('full_name', '') or ''
    additional_info = app_details.get('aditional_info', '') or ''
    app_name = app_details.get('app_name', '') or ''
    
    # Use first non-empty value
    if full_name.strip():
        return full_name.strip()
    elif additional_info.strip():
        return additional_info.strip()
    else:
        return app_name.strip()

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch TimeCamp computer activities and save as JSON",
        epilog="By default, fetches data for yesterday unless specified otherwise."
    )
    parser.add_argument("--from", dest="from_date", default="yesterday",
                      help="Start date (YYYY-MM-DD format or 'yesterday'). Default: yesterday")
    parser.add_argument("--to", dest="to_date", default="yesterday",
                      help="End date (YYYY-MM-DD format or 'yesterday'). Default: yesterday")
    parser.add_argument("--output", default="timecamp_computer_time_data.jsonl",
                      help="Output file path. Default: timecamp_computer_time_data.jsonl")
    parser.add_argument("--format", choices=["json", "jsonl"], default="jsonl",
                      help="Output format: json (pretty) or jsonl (newline-delimited). Default: jsonl")
    parser.add_argument("--include", default="application,window_title",
                      help="Additional data to include: application, window_title. Default: application,window_title")
    parser.add_argument("--user-ids", 
                      help="User IDs to filter by (comma-separated). If not specified, fetches for all users")
    parser.add_argument("--enrich-applications", action="store_true", default=True,
                      help="Enrich activities with application details (default: enabled)")
    parser.add_argument("--no-enrich-applications", dest="enrich_applications", action="store_false",
                      help="Disable application enrichment for faster execution")
    parser.add_argument("--debug", action="store_true",
                      help="Enable debug logging")
    
    return parser.parse_args()

def setup_environment(debug=False):
    """Set up the environment and return API client."""
    # Set up logger
    logger = setup_logger('timecamp_computer_data', debug)
    
    # Load environment variables
    load_dotenv()
    
    # Initialize TimeCamp API client
    config = TimeCampConfig.from_env()
    timecamp_api = TimeCampAPI(config, debug)
    
    return logger, timecamp_api

def generate_date_range(from_date, to_date):
    """Generate list of dates between from_date and to_date (inclusive)."""
    from_date_parsed = parse_date(from_date)
    to_date_parsed = parse_date(to_date)
    
    # Convert strings to datetime objects for iteration
    start_date = datetime.strptime(from_date_parsed, '%Y-%m-%d')
    end_date = datetime.strptime(to_date_parsed, '%Y-%m-%d')
    
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    return dates

def fetch_computer_activities(api, from_date, to_date, include, user_ids, logger):
    """Fetch computer activities from TimeCamp."""
    # Generate list of dates in the range
    dates = generate_date_range(from_date, to_date)
    
    logger.info(f"Fetching TimeCamp computer activities for {len(dates)} dates: {dates[0]} to {dates[-1]}")
    if include:
        logger.info(f"Including additional data: {include}")
    
    # Parse user IDs if provided, otherwise fetch all available user IDs
    user_id_list = None
    if user_ids:
        try:
            user_id_list = [int(uid.strip()) for uid in user_ids.split(',')]
            logger.info(f"Filtering by specified user IDs: {', '.join(map(str, user_id_list))}")
        except ValueError as e:
            raise ValueError(f"Invalid user ID format: {e}")
    else:
        # If no user IDs specified, fetch all available user IDs
        logger.info("No user IDs specified, fetching all available user IDs from the system")
        user_details = api.get_user_details()
        all_user_ids = []
        
        # Extract user IDs from the user details (remove 'u' prefix if present)
        for user_id in user_details.get('users', {}).keys():
            numeric_id = user_id[1:] if user_id.startswith('u') else user_id
            try:
                all_user_ids.append(int(numeric_id))
            except ValueError:
                logger.warning(f"Skipping invalid user ID: {user_id}")
        
        user_id_list = all_user_ids
        logger.info(f"Found {len(user_id_list)} users, fetching computer activities for all users: {user_id_list[:10]}{'...' if len(user_id_list) > 10 else ''}")
    
    # Fetch computer activities from the API (batching is handled automatically by the API)
    activities = api.get_computer_activities(
        dates=dates,
        include=include,
        user_ids=user_id_list
    )
    
    logger.info(f"Retrieved {len(activities)} computer activities")
    return activities

def enrich_activities_with_user_details(activities, api, logger):
    """Add user details to each computer activity."""
    logger.info("Fetching user details to enrich computer activities")
    
    # Get user details from API
    user_details = api.get_user_details()
    
    # Log some debug information about the structure
    if logger.isEnabledFor(10):  # DEBUG level
        logger.debug(f"User details keys: {', '.join(user_details.keys())}")
        if 'users' in user_details:
            logger.debug(f"Number of users: {len(user_details['users'])}")
            logger.debug(f"First few user IDs: {list(user_details['users'].keys())[:5]}")
    
    # Create user info lookup dictionary
    user_info = {}
    
    # Extract and organize user information
    for user_id, user_data in user_details.get('users', {}).items():
        # Store both with and without 'u' prefix for matching
        numeric_id = user_id
        if user_id.startswith('u'):
            numeric_id = user_id[1:]  # Remove 'u' prefix
        
        user_info[user_id] = {
            'email': user_data.get('email', ''),
            'display_name': user_data.get('display_name', ''),
            'groups': {}
        }
        
        # Also store with numeric ID for direct matching with activities
        if numeric_id != user_id:
            user_info[numeric_id] = user_info[user_id]
    
    # Extract group information similar to time entries script
    groups_data = user_details.get('groups', {})
    
    # Function to recursively get breadcrumb path
    def get_breadcrumb_path(group_id, level=0):
        if group_id not in groups_data:
            # Try without 'g' prefix if has it
            if group_id.startswith('g'):
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
        name = group.get('name', '')
        
        # Get parent_id from the group data
        parent_id = group.get('parent_id')
        
        # Check if this is a root group (parent_id is "0" or none)
        if not parent_id or parent_id == "0":
            return [name]
        
        # Get the parent path recursively, then append current group name
        parent_path = get_breadcrumb_path(f"g{parent_id}" if not parent_id.startswith('g') else parent_id, level + 1)
        return parent_path + [name]
    
    # Build group breadcrumbs and assign to users
    for group_id, group_data in groups_data.items():
        breadcrumb_path = get_breadcrumb_path(group_id)
        
        # For each user in this group, add the group info
        users = group_data.get('users', {})
        
        # Handle different format of users (dict vs list)
        if isinstance(users, dict):
            for user_id in users.keys():
                if user_id in user_info:
                    user_info[user_id]['groups'][group_id] = {
                        'group_name': group_data.get('name', ''),
                        'breadcrumb_path': breadcrumb_path
                    }
    
    # Enrich activities with user information
    for activity in activities:
        user_id = activity.get('user_id')
        if user_id and user_id in user_info:
            activity['email'] = user_info[user_id]['email']
            activity['display_name'] = user_info[user_id]['display_name']
            
            # Get group information for the user
            user_groups = user_info[user_id]['groups']
            if user_groups:
                # Use the first group for now (most users have only one primary group)
                first_group_id = next(iter(user_groups))
                group_data = user_groups[first_group_id]
                
                activity['group_name'] = group_data['group_name']
                
                # Add breadcrumb levels
                breadcrumb_path = group_data['breadcrumb_path']
                for i in range(4):
                    if i < len(breadcrumb_path):
                        activity[f'group_breadcrumb_level_{i+1}'] = breadcrumb_path[i]
                    else:
                        activity[f'group_breadcrumb_level_{i+1}'] = ''
        else:
            # Default values if user not found
            activity['email'] = ''
            activity['display_name'] = ''
            activity['group_name'] = ''
            for i in range(1, 5):
                activity[f'group_breadcrumb_level_{i}'] = ''
    
    logger.info("Computer activities enriched with user details")
    return activities

def enrich_activities_with_application_details(activities, api, logger):
    """Add application details to each computer activity."""
    logger.info("Fetching application details to enrich computer activities")
    
    # Collect all unique application IDs from activities
    application_ids = set()
    for activity in activities:
        app_id = activity.get('application_id')
        if app_id and app_id != '0':  # Skip null and '0' application IDs
            application_ids.add(str(app_id))
    
    if not application_ids:
        logger.info("No application IDs found in activities to enrich")
        return activities
    
    logger.info(f"Found {len(application_ids)} unique application IDs to fetch details for")
    
    # Fetch application details in batches (using cache)
    applications = api.get_applications_with_cache(list(application_ids), batch_size=200)
    
    # Log some debug information about the applications structure
    if logger.isEnabledFor(10):  # DEBUG level
        logger.debug(f"Retrieved application details for {len(applications)} applications")
        if applications:
            sample_app_id = next(iter(applications))
            sample_app = applications[sample_app_id]
            logger.debug(f"Sample application keys: {', '.join(sample_app.keys())}")
    
    # Get category mapping
    category_mapping = get_category_mapping()
    
    # Enrich activities with application information
    enriched_count = 0
    for activity in activities:
        app_id = activity.get('application_id')
        if app_id and str(app_id) in applications:
            app_details = applications[str(app_id)]
            
            # Use fallback logic for application name
            activity['application_name'] = get_application_name_fallback(app_details)
            
            # Map category ID to category name
            category_id = app_details.get('category_id', '0')
            activity['category_name'] = category_mapping.get(category_id, 'No category')
            
            enriched_count += 1
        else:
            # Default values if application not found
            activity['application_name'] = ''
            activity['category_name'] = 'No category'
    
    logger.info(f"Computer activities enriched with application details ({enriched_count}/{len(activities)} activities enriched)")
    return activities

def transform_to_final_format(activities, logger):
    """Transform activities to final format with only specified columns."""
    logger.info("Transforming activities to final format with specified columns")
    
    final_activities = []
    for activity in activities:
        # Calculate start_time from end_time and time_span
        end_time = activity.get('end_time', '')
        time_span = activity.get('time_span', 0)
        start_time = calculate_start_time(end_time, time_span)
        
        # Create final record with only specified columns
        final_activity = {
            'user_id': activity.get('user_id', ''),
            'application_id': activity.get('application_id', ''),
            'start_time': start_time,
            'end_time': end_time,
            'time_span': time_span,
            'window_title_id': activity.get('window_title_id', ''),
            'application_name': activity.get('application_name', ''),
            'window_title': activity.get('window_title', ''),
            'user_group_name': activity.get('group_name', ''),
            'user_email': activity.get('email', ''),
            'user_name': activity.get('display_name', ''),
            'category_name': activity.get('category_name', 'No category')
        }
        
        final_activities.append(final_activity)
    
    logger.info(f"Transformed {len(final_activities)} activities to final format")
    return final_activities

def save_to_file(activities, output_path, format_type, logger):
    """Save computer activities to a file.
    
    Args:
        activities: List of computer activity dictionaries
        output_path: Path to save the file
        format_type: 'json' for pretty JSON, 'jsonl' for newline-delimited JSON
        logger: Logger object
    """
    # Make sure the directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    
    with open(output_path, 'w') as f:
        if format_type == 'json':
            # Pretty JSON format
            json.dump(activities, f, indent=2)
        else:  # jsonl format
            # Newline-delimited JSON format (one JSON object per line)
            for activity in activities:
                f.write(json.dumps(activity) + '\n')
    
    logger.info(f"Computer activities saved to {output_path} in {format_type} format")

def main():
    """Main function."""
    # Parse command-line arguments
    args = parse_arguments()
    
    # Set up environment
    logger, api = setup_environment(args.debug)
    
    try:
        # Fetch computer activities
        activities = fetch_computer_activities(
            api, 
            args.from_date, 
            args.to_date,
            args.include,
            args.user_ids,
            logger
        )
        
        # Enrich activities with user details
        enriched_activities = enrich_activities_with_user_details(activities, api, logger)
        
        # Enrich activities with application details if requested
        if args.enrich_applications:
            enriched_activities = enrich_activities_with_application_details(enriched_activities, api, logger)
        else:
            logger.info("Skipping application enrichment (disabled by --no-enrich-applications)")
            # Add empty application fields for consistency
            for activity in enriched_activities:
                activity['application_name'] = ''
                activity['category_name'] = 'No category'
        
        # Transform to final format with only specified columns
        final_activities = transform_to_final_format(enriched_activities, logger)
        
        # Save to file in specified format
        save_to_file(final_activities, args.output, args.format, logger)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main() 