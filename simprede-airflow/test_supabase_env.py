#!/usr/bin/env python3
"""
Test script to verify Supabase environment variables are loaded correctly
Run this inside the Airflow container to debug environment variable issues
"""

import os

def test_environment_variables():
    print("🔍 Testing Supabase Environment Variables")
    print("=" * 50)
    
    # Get raw environment variables
    raw_vars = {
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD'),
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_PORT': os.getenv('DB_PORT'),
        'DB_NAME': os.getenv('DB_NAME')
    }
    
    print("\n📋 Raw environment variables:")
    for key, value in raw_vars.items():
        if key == 'DB_PASSWORD':
            display_value = '***REDACTED***' if value else 'NOT_SET'
        else:
            display_value = f"'{value}'" if value else 'NOT_SET'
        print(f"  {key}: {display_value}")
    
    # Process variables (remove quotes)
    processed_vars = {}
    for key, value in raw_vars.items():
        if value:
            processed_vars[key] = value.strip().strip('"\'')
        else:
            processed_vars[key] = ''
    
    print("\n🧹 Processed variables (quotes removed):")
    for key, value in processed_vars.items():
        if key == 'DB_PASSWORD':
            display_value = '***REDACTED***' if value else 'EMPTY'
        else:
            display_value = f"'{value}'" if value else 'EMPTY'
        print(f"  {key}: {display_value}")
    
    # Check if all required variables are present
    required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST']
    missing_vars = []
    
    for var in required_vars:
        if not processed_vars.get(var):
            missing_vars.append(var)
    
    print(f"\n🔍 Validation Results:")
    if not missing_vars:
        print("✅ All required Supabase credentials are available!")
        print("🚀 The DAG should be able to connect to Supabase")
        
        # Test connection string format
        db_user = processed_vars['DB_USER']
        db_host = processed_vars['DB_HOST']
        db_port = processed_vars.get('DB_PORT', '6543')
        db_name = processed_vars.get('DB_NAME', 'postgres')
        
        print(f"\n🔗 Connection details:")
        print(f"  Host: {db_host}")
        print(f"  Port: {db_port}")
        print(f"  User: {db_user}")
        print(f"  Database: {db_name}")
        
    else:
        print(f"❌ Missing required variables: {', '.join(missing_vars)}")
        print("\n🔧 Troubleshooting:")
        print("1. Check if .env file exists in project root")
        print("2. Restart Docker containers: docker compose down && docker compose up")
        print("3. Verify docker-compose.yml loads environment variables")
    
    return len(missing_vars) == 0

if __name__ == "__main__":
    success = test_environment_variables()
    exit(0 if success else 1)