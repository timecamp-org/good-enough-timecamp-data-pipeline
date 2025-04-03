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
        epilog="By default, fetches data for yesterday unless specified otherwise."
    )
    parser.add_argument("--from", dest="from_date", default="yesterday",
                      help="Start date (YYYY-MM-DD format or 'yesterday'). Default: yesterday")
    parser.add_argument("--to", dest="to_date", default="yesterday",
                      help="End date (YYYY-MM-DD format or 'yesterday'). Default: yesterday")
    parser.add_argument("--output", default=None,
                      help="Output file path. Default: timecamp_data.jsonl")
    parser.add_argument("--format", choices=["json", "jsonl"], default="jsonl",
                      help="Output format: json (pretty) or jsonl (newline-delimited). Default: jsonl")
    parser.add_argument("--debug", action="store_true",
                      help="Enable debug logging")
    
    return parser.parse_args()

def setup_environment(debug=False):
    """Set up the environment and return API client."""
    # Set up logger
    logger = setup_logger('timecamp_data', debug)
    
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
    
    logger.info(f"Fetching TimeCamp time entries from {from_date_parsed} to {to_date_parsed}")
    
    # Fetch time entries from the API
    entries = api.get_time_entries(from_date_parsed, to_date_parsed)
    
    logger.info(f"Retrieved {len(entries)} time entries")
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
    
    with open(output_path, 'w') as f:
        if format_type == 'json':
            # Pretty JSON format
            json.dump(entries, f, indent=2)
        else:  # jsonl format
            # Newline-delimited JSON format (one JSON object per line)
            for entry in entries:
                f.write(json.dumps(entry) + '\n')
    
    logger.info(f"Time entries saved to {output_path} in {format_type} format")

def main():
    """Main function."""
    # Parse command-line arguments
    args = parse_arguments()
    
    # Set up environment
    logger, api = setup_environment(args.debug)
    
    try:
        # Fetch time entries
        entries = fetch_time_entries(api, args.from_date, args.to_date, logger)
        
        # Generate default output filename if not specified
        if args.output is None:
            # Use appropriate extension based on format
            extension = ".json" if args.format == "json" else ".jsonl"
            args.output = f"timecamp_data{extension}"
        
        # Save to file in specified format
        save_to_file(entries, args.output, args.format, logger)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main() 