#!/usr/bin/env python3
"""
Test script to verify Supabase connection is working
"""
import sys
import os

# Add the utils directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

try:
    from supabase_connector import SupabaseConnection
    
    print("🔄 Testing Supabase connection...")
    
    # Initialize connection
    conn = SupabaseConnection()
    print("✅ SupabaseConnection initialized successfully")
    
    # Test table counts
    counts = conn.get_eswd_table_counts()
    print(f"📊 Table counts: {counts}")
    
    # Test loading some data
    print("🔄 Testing data loading...")
    
    # Test scraper data
    scraper_data = conn.get_ocorrencias_data()
    print(f"📈 Google scraper data: {len(scraper_data)} records")
    
    # Test ESWD data
    eswd_data = conn.get_eswd_data_paginated()
    print(f"📈 ESWD data: {len(eswd_data)} records")
    
    print("🎉 All tests passed! Supabase connection is working correctly.")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
    print("💡 Make sure:")
    print("   - The .env file exists in the project root")
    print("   - SUPABASE_URL and SUPABASE_ANON_KEY are set in .env")
    print("   - python-dotenv and supabase packages are installed")
    sys.exit(1)
