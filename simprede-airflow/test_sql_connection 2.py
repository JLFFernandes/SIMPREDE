#!/usr/bin/env python3
"""
Simple test script to verify SQL connectivity to Supabase
This can be run manually to test the connection before running the full DAG
"""

import os
import sys
import psycopg2
from datetime import datetime

def test_supabase_connection():
    """Test direct connection to Supabase with current environment variables"""
    
    print("🔍 Testing Supabase Connection")
    print("=" * 40)
    
    # Get credentials from environment
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '6543'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'sslmode': 'require'
    }
    
    print("📋 Connection details:")
    print(f"  Host: {db_config['host']}")
    print(f"  Port: {db_config['port']}")
    print(f"  Database: {db_config['database']}")
    print(f"  User: {db_config['user']}")
    print(f"  SSL Mode: {db_config['sslmode']}")
    
    # Check if required credentials are present
    if not all([db_config['host'], db_config['user'], db_config['password']]):
        print("❌ Missing required credentials")
        return False
    
    try:
        # Connect to database
        print("\n🔗 Connecting to Supabase...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Test basic connectivity
        cursor.execute("SELECT current_timestamp, current_user, current_database()")
        result = cursor.fetchone()
        print("✅ Connection successful!")
        print(f"  Timestamp: {result[0]}")
        print(f"  User: {result[1]}")
        print(f"  Database: {result[2]}")
        
        # Check google_scraper schema
        print("\n🔍 Checking google_scraper schema...")
        cursor.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = 'google_scraper'
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        if tables:
            print(f"✅ Found {len(tables)} tables in google_scraper schema:")
            staging_tables = []
            permanent_tables = []
            
            for table_name, table_type in tables:
                print(f"  - {table_name} ({table_type})")
                if 'staging' in table_name.lower():
                    staging_tables.append(table_name)
                else:
                    permanent_tables.append(table_name)
            
            # Show recent staging tables
            if staging_tables:
                print(f"\n📊 Staging tables ({len(staging_tables)}):")
                for table in staging_tables[-5:]:  # Show last 5
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM google_scraper.{table}")
                        count = cursor.fetchone()[0]
                        print(f"  - {table}: {count} records")
                    except Exception as e:
                        print(f"  - {table}: Error counting ({e})")
            
            # Check if main artigos_filtrados table exists
            if 'artigos_filtrados' in [t[0] for t in tables]:
                cursor.execute("SELECT COUNT(*) FROM google_scraper.artigos_filtrados")
                main_count = cursor.fetchone()[0]
                print(f"\n📈 Main artigos_filtrados table: {main_count} total records")
        else:
            print("⚠️ No tables found in google_scraper schema")
        
        conn.close()
        print("\n✅ Test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Connection test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_supabase_connection()
    sys.exit(0 if success else 1)