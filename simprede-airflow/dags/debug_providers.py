#!/usr/bin/env python3
"""
Debug script to test PostgreSQL provider availability
"""
import sys
import traceback

print("🔍 Debugging PostgreSQL provider import...")
print(f"Python version: {sys.version}")
print(f"Python path: {sys.executable}")

# Test 1: Basic import test
try:
    import airflow
    print(f"✅ Airflow imported: {airflow.__version__}")
except Exception as e:
    print(f"❌ Failed to import Airflow: {e}")

# Test 2: Check if provider package is installed
try:
    import pkg_resources
    postgres_provider = pkg_resources.get_distribution('apache-airflow-providers-postgres')
    print(f"✅ PostgreSQL provider package found: {postgres_provider.version}")
except Exception as e:
    print(f"❌ PostgreSQL provider package not found: {e}")

# Test 3: Try importing the operators
try:
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    print("✅ PostgresOperator imported successfully")
except Exception as e:
    print(f"❌ Failed to import PostgresOperator: {e}")
    traceback.print_exc()

# Test 4: Try importing the hooks
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    print("✅ PostgresHook imported successfully")
except Exception as e:
    print(f"❌ Failed to import PostgresHook: {e}")
    traceback.print_exc()

# Test 5: Check what's available in the providers namespace
try:
    import airflow.providers
    print(f"✅ Providers namespace available at: {airflow.providers.__path__}")
    
    import os
    providers_dir = airflow.providers.__path__[0]
    postgres_dir = os.path.join(providers_dir, 'postgres')
    if os.path.exists(postgres_dir):
        print(f"✅ PostgreSQL provider directory exists: {postgres_dir}")
        files = os.listdir(postgres_dir)
        print(f"   Files: {files}")
    else:
        print(f"❌ PostgreSQL provider directory not found: {postgres_dir}")
        
except Exception as e:
    print(f"❌ Failed to check providers namespace: {e}")
    traceback.print_exc()

print("🏁 Debug complete")