#!/usr/bin/env python3
"""
JSONL to CSV Converter

A simple script that converts JSONL (JSON Lines) files to CSV format.
Handles nested JSON objects by converting them to JSON strings.

Usage:
    python jsonl_to_csv.py input_file.jsonl
    
Output:
    Creates input_file.csv in the same directory
"""

import json
import csv
import sys
import os
from pathlib import Path
from typing import Dict, Any, List, Union


def flatten_json_value(value: Any) -> str:
    """
    Convert a JSON value to a string suitable for CSV.
    
    Args:
        value: Any JSON-serializable value
        
    Returns:
        String representation of the value
    """
    if value is None:
        return ""
    elif isinstance(value, (dict, list)):
        # Convert complex objects to JSON strings
        return json.dumps(value, ensure_ascii=False)
    elif isinstance(value, bool):
        return str(value).lower()
    else:
        return str(value)


def get_all_keys(jsonl_file: str) -> List[str]:
    """
    Scan the JSONL file to get all unique keys across all records.
    This ensures we capture all columns that might appear in any record.
    
    Args:
        jsonl_file: Path to the JSONL file
        
    Returns:
        List of all unique keys found in the file
    """
    all_keys = set()
    
    try:
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    if isinstance(record, dict):
                        all_keys.update(record.keys())
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON on line {line_num}: {e}")
                    
    except FileNotFoundError:
        print(f"Error: File '{jsonl_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{jsonl_file}': {e}")
        sys.exit(1)
        
    return sorted(list(all_keys))


def convert_jsonl_to_csv(jsonl_file: str, csv_file: str) -> None:
    """
    Convert a JSONL file to CSV format.
    
    Args:
        jsonl_file: Path to the input JSONL file
        csv_file: Path to the output CSV file
    """
    print(f"Converting '{jsonl_file}' to '{csv_file}'...")
    
    # First pass: get all possible column names
    all_keys = get_all_keys(jsonl_file)
    
    if not all_keys:
        print("Warning: No valid JSON records found in the file.")
        return
    
    print(f"Found {len(all_keys)} unique columns")
    
    # Second pass: convert data
    records_processed = 0
    records_skipped = 0
    
    try:
        with open(jsonl_file, 'r', encoding='utf-8') as infile, \
             open(csv_file, 'w', newline='', encoding='utf-8') as outfile:
            
            writer = csv.DictWriter(outfile, fieldnames=all_keys, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    if isinstance(record, dict):
                        # Flatten the record for CSV
                        csv_record = {}
                        for key in all_keys:
                            csv_record[key] = flatten_json_value(record.get(key))
                        
                        writer.writerow(csv_record)
                        records_processed += 1
                    else:
                        print(f"Warning: Skipping non-object JSON on line {line_num}")
                        records_skipped += 1
                        
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON on line {line_num}: {e}")
                    records_skipped += 1
                    
    except Exception as e:
        print(f"Error during conversion: {e}")
        sys.exit(1)
    
    print(f"Conversion complete!")
    print(f"  Records processed: {records_processed}")
    print(f"  Records skipped: {records_skipped}")
    print(f"  Output file: {csv_file}")


def main():
    """Main function to handle command line arguments and orchestrate the conversion."""
    if len(sys.argv) != 2:
        print("Usage: python jsonl_to_csv.py <input_file.jsonl>")
        print("\nExample:")
        print("  python jsonl_to_csv.py timecamp_data.jsonl")
        print("  # Creates timecamp_data.csv")
        sys.exit(1)
    
    jsonl_file = sys.argv[1]
    
    # Validate input file
    if not os.path.isfile(jsonl_file):
        print(f"Error: File '{jsonl_file}' does not exist.")
        sys.exit(1)
    
    # Generate output filename (same name, different extension)
    path = Path(jsonl_file)
    csv_file = str(path.with_suffix('.csv'))
    
    # Check if output file already exists
    if os.path.exists(csv_file):
        response = input(f"Output file '{csv_file}' already exists. Overwrite? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            print("Conversion cancelled.")
            sys.exit(0)
    
    # Perform the conversion
    convert_jsonl_to_csv(jsonl_file, csv_file)


if __name__ == "__main__":
    main() 