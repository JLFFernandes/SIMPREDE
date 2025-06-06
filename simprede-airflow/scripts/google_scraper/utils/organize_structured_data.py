#!/usr/bin/env python3
"""
Script to fix the structured directory organization by copying files to year/month/day structure.
"""

import os
import shutil
from datetime import datetime
import sys

def organize_structured_data():
    """
    Organize existing structured data into year/month/day folders.
    """
    # Get current date components
    current_year = datetime.now().strftime("%Y")
    current_month = datetime.now().strftime("%m")
    current_day = datetime.now().strftime("%d")
    
    # Get the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Define directories
    structured_dir = os.path.join(project_root, "data", "structured")
    year_month_day_dir = os.path.join(structured_dir, current_year, current_month, current_day)
    
    print(f"Looking for CSV files in: {structured_dir}")
    
    if not os.path.exists(structured_dir):
        print(f"⚠️ Structured directory not found: {structured_dir}")
        return
    
    # Create year/month/day directory if it doesn't exist
    os.makedirs(year_month_day_dir, exist_ok=True)
    print(f"Created directory: {year_month_day_dir}")
    
    # Find all CSV files in the structured directory (not in subdirectories)
    csv_files = [f for f in os.listdir(structured_dir) if f.endswith('.csv') and os.path.isfile(os.path.join(structured_dir, f))]
    print(f"Found {len(csv_files)} CSV files: {csv_files}")
    
    # Copy each file to the year/month/day directory
    for file in csv_files:
        source_path = os.path.join(structured_dir, file)
        target_path = os.path.join(year_month_day_dir, file)
        
        # Copy the file
        shutil.copy2(source_path, target_path)
        print(f"✅ Copied {file} to {year_month_day_dir}")
    
    print(f"\n✅ All CSV files in {structured_dir} have been copied to {year_month_day_dir}")
    print(f"✅ Total files copied: {len(csv_files)}")

if __name__ == "__main__":
    organize_structured_data()
