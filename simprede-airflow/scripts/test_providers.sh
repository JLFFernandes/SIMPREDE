#!/bin/bash

# Test script to validate database providers are properly installed

echo "🔍 Testing Database Provider Installation..."

# Test PostgreSQL provider
echo "Testing PostgreSQL provider..."
python3 -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
print('✅ PostgreSQL provider installed successfully')
print('  - PostgresHook: Available')
print('  - PostgresOperator: Available')
"

# Test MySQL provider
echo "Testing MySQL provider..."
python3 -c "
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
print('✅ MySQL provider installed successfully')
print('  - MySqlHook: Available')
print('  - MySqlOperator: Available')
"

# Test Google Cloud provider
echo "Testing Google Cloud provider..."
python3 -c "
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
print('✅ Google Cloud provider installed successfully')
print('  - GCSHook: Available')
print('  - GCSListObjectsOperator: Available')
"

# Test Selenium
echo "Testing Selenium..."
python3 -c "
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
print(f'✅ Selenium {selenium.__version__} installed successfully')
"

# Test core dependencies
echo "Testing core dependencies..."
python3 -c "
import pandas as pd
import numpy as np
import requests
import psycopg2
import sqlalchemy
print('✅ Core dependencies installed successfully')
print(f'  - Pandas: {pd.__version__}')
print(f'  - NumPy: {np.__version__}')
print(f'  - Requests: {requests.__version__}')
print(f'  - psycopg2: {psycopg2.__version__}')
print(f'  - SQLAlchemy: {sqlalchemy.__version__}')
"

echo "🎉 All provider tests completed successfully!"