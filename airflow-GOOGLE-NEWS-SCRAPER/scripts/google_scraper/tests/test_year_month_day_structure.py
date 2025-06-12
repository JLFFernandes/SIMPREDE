#!/usr/bin/env python3
"""
Test script to verify the new file path structure for artigos_google_municipios_pt CSV files.
This script simulates the file saving with year/month/day structure.
"""

import os
import pandas as pd
from datetime import datetime
import sys

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Get current date components
current_date = datetime.now().strftime("%Y%m%d")
current_year = datetime.now().strftime("%Y")
current_month = datetime.now().strftime("%m")
current_day = datetime.now().strftime("%d")

# Base data directory
base_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

# Create the directory structure
raw_data_dir = os.path.join(base_dir, "raw", current_year, current_month, current_day)
os.makedirs(raw_data_dir, exist_ok=True)

# Path to output CSV in raw data year/month/day structure
raw_output_csv = os.path.join(raw_data_dir, f"artigos_google_municipios_pt_{current_date}.csv")

# Path to output CSV in structured directory for compatibility
structured_dir = os.path.join(base_dir, "structured")
structured_output_csv = os.path.join(structured_dir, f"artigos_google_municipios_pt_{current_date}.csv")
standard_output_csv = os.path.join(structured_dir, "artigos_google_municipios_pt.csv")

# Create a sample DataFrame
sample_data = {
    "ID": ["test1", "test2", "test3"],
    "type": ["flood", "fire", "earthquake"],
    "subtype": ["flash", "wildfire", "tremor"],
    "date": ["01/01/2025", "02/01/2025", "03/01/2025"],
    "year": [2025, 2025, 2025],
    "month": [1, 1, 1],
    "day": [1, 2, 3],
    "hour": ["12:00", "13:00", "14:00"],
    "georef": ["Lisbon", "Porto", "Faro"],
    "district": ["Lisbon", "Porto", "Faro"],
    "municipali": ["Lisbon", "Porto", "Faro"],
    "parish": ["Lisbon", "Porto", "Faro"],
    "DICOFREG": ["", "", ""],
    "source": ["news1", "news2", "news3"],
    "sourcedate": ["2025-01-01", "2025-01-02", "2025-01-03"],
    "sourcetype": ["web", "web", "web"],
    "page": ["http://test1.com", "http://test2.com", "http://test3.com"],
    "fatalities": [1, 0, 3],
    "injured": [2, 1, 5],
    "evacuated": [10, 20, 30],
    "displaced": [5, 10, 15],
    "missing": [0, 2, 1]
}

df = pd.DataFrame(sample_data)

# Save CSV files
print(f"Saving test CSV to raw data directory: {raw_output_csv}")
df.to_csv(raw_output_csv, index=False)

print(f"Saving test CSV to structured directory: {structured_output_csv}")
df.to_csv(structured_output_csv, index=False)

print(f"Saving test CSV to standard path: {standard_output_csv}")
df.to_csv(standard_output_csv, index=False)

print("\nVerifying files:")
for path in [raw_output_csv, structured_output_csv, standard_output_csv]:
    if os.path.exists(path):
        size = os.path.getsize(path)
        print(f"✅ {path} - {size} bytes")
    else:
        print(f"❌ {path} - File not found")

print(f"\nDirectory structure created: {raw_data_dir}")
