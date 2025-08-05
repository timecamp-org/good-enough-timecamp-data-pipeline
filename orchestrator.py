#!/usr/bin/env python3
"""
TimeCamp Data Pipeline Orchestrator
Simple script to run the complete ETL pipeline:
1. Fetch data from TimeCamp API → Upload to GCS
2. Load data from GCS → BigQuery
"""

import subprocess
import sys
import time
from datetime import datetime

def run_script(script_name, description):
    """Run a Python script and return the result."""
    print(f"\n{'='*60}")
    print(f"🚀 STARTING: {description}")
    print(f"Script: {script_name}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        # Run the script and capture output
        result = subprocess.run(
            [sys.executable, script_name], 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ SUCCESS: {description}")
        print(f"Duration: {duration:.2f} seconds")
        
        # Print script output
        if result.stdout:
            print(f"\n--- OUTPUT ---")
            print(result.stdout)
        
        if result.stderr:
            print(f"\n--- WARNINGS ---")
            print(result.stderr)
            
        return True, duration
        
    except subprocess.CalledProcessError as e:
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"❌ FAILED: {description}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Exit code: {e.returncode}")
        
        if e.stdout:
            print(f"\n--- OUTPUT ---")
            print(e.stdout)
            
        if e.stderr:
            print(f"\n--- ERROR ---")
            print(e.stderr)
            
        return False, duration

def main():
    """Main orchestrator function."""
    pipeline_start = time.time()
    
    print("🎯 TimeCamp Data Pipeline Orchestrator")
    print(f"Pipeline started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Fetch data from TimeCamp and upload to GCS
    success1, duration1 = run_script(
        "fetch_timecamp_data.py", 
        "Fetch TimeCamp Data → Upload to GCS"
    )
    
    if not success1:
        print(f"\n💥 PIPELINE FAILED at Step 1 - Aborting")
        return False
    
    # Step 2: Load data from GCS to BigQuery
    success2, duration2 = run_script(
        "destination_googlebigquery.py",
        "Load Data from GCS → BigQuery"
    )
    
    if not success2:
        print(f"\n💥 PIPELINE FAILED at Step 2")
        return False
    
    # Pipeline summary
    pipeline_end = time.time()
    total_duration = pipeline_end - pipeline_start
    
    print(f"\n{'='*60}")
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"{'='*60}")
    print(f"Step 1 (Fetch → GCS):     {duration1:.2f} seconds")
    print(f"Step 2 (GCS → BigQuery):   {duration2:.2f} seconds")
    print(f"Total Pipeline Time:       {total_duration:.2f} seconds")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n💥 Unexpected error in orchestrator: {str(e)}")
        sys.exit(1)