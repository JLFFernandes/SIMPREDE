#!/bin/bash

# Debug script to check environment variables in Airflow container

echo "🔍 Environment Variable Debug Script"
echo "==================================="

echo ""
echo "📋 Checking database credentials:"
echo "DB_USER: ${DB_USER:-'❌ Not set'}"
echo "DB_PASSWORD: ${DB_PASSWORD:-'❌ Not set'}"
echo "DB_HOST: ${DB_HOST:-'❌ Not set'}"
echo "DB_PORT: ${DB_PORT:-'❌ Not set'}"
echo "DB_NAME: ${DB_NAME:-'❌ Not set'}"

echo ""
echo "📋 Checking GCS configuration:"
echo "GCS_PROJECT_ID: ${GCS_PROJECT_ID:-'❌ Not set'}"
echo "GCS_BUCKET_NAME: ${GCS_BUCKET_NAME:-'❌ Not set'}"

echo ""
echo "📋 Checking Supabase configuration:"
echo "SUPABASE_URL: ${SUPABASE_URL:-'❌ Not set'}"
echo "SUPABASE_ANON_KEY: ${SUPABASE_ANON_KEY:-'❌ Not set (length)'}"

echo ""
echo "📋 All environment variables containing 'DB_':"
env | grep DB_ | sort

echo ""
echo "📋 Testing Python access to environment variables:"
python3 -c "
import os
print('Python os.getenv() test:')
print(f'  DB_USER: {os.getenv(\"DB_USER\", \"Not found\")}')
print(f'  DB_PASSWORD: {\"***\" if os.getenv(\"DB_PASSWORD\") else \"Not found\"}')
print(f'  DB_HOST: {os.getenv(\"DB_HOST\", \"Not found\")}')
"

echo ""
echo "🔍 Debug completed!"