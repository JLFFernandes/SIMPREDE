#!/usr/bin/env python3
"""
Test script to verify DAG imports without timeout
"""

import sys
import time
import importlib.util

def test_dag_import():
    """Test importing the DAG file to check for timeout issues"""
    start_time = time.time()
    
    try:
        # Import the DAG file
        spec = importlib.util.spec_from_file_location(
            "google_scraper_dag", 
            "/Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow/dags/google_scraper_dag.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        end_time = time.time()
        import_time = end_time - start_time
        
        print(f"✅ DAG import successful in {import_time:.2f} seconds")
        
        # Check if the DAG exists
        if hasattr(module, 'dag'):
            print(f"✅ DAG object found: {module.dag.dag_id}")
        else:
            print("⚠️ No 'dag' object found in module")
            
        return True
        
    except Exception as e:
        end_time = time.time()
        import_time = end_time - start_time
        print(f"❌ DAG import failed after {import_time:.2f} seconds: {e}")
        return False

if __name__ == "__main__":
    print("Testing DAG import...")
    success = test_dag_import()
    sys.exit(0 if success else 1)
