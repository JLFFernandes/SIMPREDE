#!/usr/bin/env python3
"""
Test script to verify the GoogleScraperPaths class works correctly
"""
import sys
import os
from datetime import datetime

# Add the DAG directory to the path to import the class
sys.path.append('/Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow/dags')

from google_scraper_dag import GoogleScraperPaths

def test_paths():
    """Test the GoogleScraperPaths class with different scenarios"""
    
    print("🧪 Testing GoogleScraperPaths class...")
    print("=" * 60)
    
    # Test 1: Current date
    print("\n📅 Test 1: Using current date")
    paths_current = GoogleScraperPaths(execution_date=datetime(2025, 6, 9))
    
    scraper_outputs = paths_current.get_scraper_outputs()
    print(f"📁 Raw directory: {paths_current.raw_dir}")
    print(f"📁 Structured directory: {paths_current.structured_dir}")
    print(f"📁 Processed directory: {paths_current.processed_dir}")
    print(f"📄 Intermediate CSV: {scraper_outputs['intermediate_csv']}")
    print(f"📄 Final CSV: {scraper_outputs['final_csv']}")
    
    # Test if files exist
    print(f"\n🔍 Checking if files exist:")
    print(f"   Intermediate CSV exists: {os.path.exists(scraper_outputs['intermediate_csv'])}")
    print(f"   Final CSV exists: {os.path.exists(scraper_outputs['final_csv'])}")
    
    # Test 2: Different execution date
    print("\n📅 Test 2: Using different date (2025-06-08)")
    paths_other = GoogleScraperPaths(execution_date=datetime(2025, 6, 8))
    
    scraper_outputs_other = paths_other.get_scraper_outputs()
    print(f"📁 Raw directory: {paths_other.raw_dir}")
    print(f"📄 Intermediate CSV: {scraper_outputs_other['intermediate_csv']}")
    print(f"   File exists: {os.path.exists(scraper_outputs_other['intermediate_csv'])}")
    
    # Test 3: Processing outputs
    print("\n📋 Test 3: Processing outputs")
    processar_outputs = paths_current.get_processar_outputs()
    for key, path in processar_outputs.items():
        print(f"   {key}: {path}")
    
    print("\n✅ Path testing completed!")

if __name__ == "__main__":
    test_paths()
