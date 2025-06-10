#!/usr/bin/env python3
"""
Simple Debug DAG for Empty Table Issue
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'simprede',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
} 

# Define the DAG
dag = DAG(
    'debug_empty_table',
    default_args=default_args,
    description='Debug why eventos table is empty',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['debug', 'eventos', 'simprede'],
    max_active_runs=1,
)

def debug_table_issue(**context):
    """Debug why the eventos table is empty"""
    print("🔍 Debugging empty eventos table issue...")
    
    # Get credentials from environment
    db_user = os.getenv('DB_USER', '').strip()
    db_password = os.getenv('DB_PASSWORD', '').strip()
    db_host = os.getenv('DB_HOST', '').strip()
    db_port = os.getenv('DB_PORT', '6543').strip()
    db_name = os.getenv('DB_NAME', 'postgres').strip()
    
    if not all([db_user, db_password, db_host]):
        print("❌ Missing database credentials")
        return
    
    # Build connection URI
    connection_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
    
    try:
        import psycopg2
        
        with psycopg2.connect(connection_uri) as conn:
            with conn.cursor() as cursor:
                
                # 1. Check if eventos table exists and has data
                print("📋 Step 1: Check eventos table status")
                cursor.execute("SELECT COUNT(*) FROM google_scraper.google_scraper_eventos")
                eventos_count = cursor.fetchone()[0]
                print(f"  - google_scraper_eventos has {eventos_count} rows")
                
                # 2. List all available tables in google_scraper schema
                print("\n📋 Step 2: List all tables in google_scraper schema")
                cursor.execute("""
                    SELECT table_name, 
                           (SELECT COUNT(*) FROM information_schema.columns 
                            WHERE table_schema = 'google_scraper' 
                            AND table_name = t.table_name) as column_count
                    FROM information_schema.tables t
                    WHERE table_schema = 'google_scraper'
                    ORDER BY table_name
                """)
                tables = cursor.fetchall()
                for table_name, col_count in tables:
                    print(f"  - {table_name} ({col_count} columns)")
                
                # 3. Check if any table has the expected columns
                print("\n📋 Step 3: Check for tables with event-like data")
                potential_source_tables = []
                for table_name, _ in tables:
                    try:
                        cursor.execute(f"""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = 'google_scraper' 
                            AND table_name = '{table_name}'
                            AND column_name IN ('ID', 'date', 'type', 'distrito', 'municipali', 'parish', 'fatalities')
                        """)
                        matching_columns = [row[0] for row in cursor.fetchall()]
                        
                        if len(matching_columns) >= 3:  # At least 3 matching columns
                            cursor.execute(f"SELECT COUNT(*) FROM google_scraper.{table_name}")
                            row_count = cursor.fetchone()[0]
                            potential_source_tables.append((table_name, len(matching_columns), row_count))
                            print(f"  - {table_name}: {len(matching_columns)} matching columns, {row_count} rows")
                    except Exception as e:
                        print(f"  - {table_name}: Error checking - {str(e)}")
                
                # 4. For the best candidate, check sample data
                if potential_source_tables:
                    # Sort by number of matching columns and row count
                    best_table = sorted(potential_source_tables, key=lambda x: (x[1], x[2]), reverse=True)[0]
                    table_name, matching_cols, row_count = best_table
                    
                    print(f"\n📋 Step 4: Analyzing best candidate table: {table_name}")
                    print(f"  - Matching columns: {matching_cols}")
                    print(f"  - Total rows: {row_count}")
                    
                    # Check column structure
                    cursor.execute(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns 
                        WHERE table_schema = 'google_scraper' 
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """)
                    columns = cursor.fetchall()
                    print("  - Column structure:")
                    for col_name, data_type, nullable in columns:
                        print(f"    • {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")
                    
                    # Check sample data
                    print("  - Sample data (first 3 rows):")
                    cursor.execute(f"SELECT * FROM google_scraper.{table_name} LIMIT 3")
                    sample_rows = cursor.fetchall()
                    for i, row in enumerate(sample_rows, 1):
                        print(f"    Row {i}: {row}")
                    
                    # Check date column issues
                    if 'date' in [col[0] for col in columns]:
                        cursor.execute(f"""
                            SELECT 
                                COUNT(*) as total_rows,
                                COUNT(CASE WHEN date IS NOT NULL THEN 1 END) as rows_with_date,
                                COUNT(CASE WHEN date IS NOT NULL AND date != '' THEN 1 END) as rows_with_valid_date
                            FROM google_scraper.{table_name}
                        """)
                        date_stats = cursor.fetchone()
                        total, with_date, with_valid_date = date_stats
                        print(f"  - Date column analysis:")
                        print(f"    • Total rows: {total}")
                        print(f"    • Rows with date NOT NULL: {with_date}")
                        print(f"    • Rows with valid date: {with_valid_date}")
                        
                        if with_valid_date == 0:
                            print("    ⚠️ NO ROWS WITH VALID DATES! This is why the table is empty.")
                            print("    💡 The WHERE af.date IS NOT NULL clause filters out all data.")
                
                # 5. Test simple insertion
                print(f"\n📋 Step 5: Test simple insertion from best table")
                if potential_source_tables:
                    best_table_name = potential_source_tables[0][0]
                    
                    try:
                        # Try inserting without date filter first
                        cursor.execute(f"""
                            INSERT INTO google_scraper.google_scraper_eventos (
                                id, type, subtype, source_name
                            )
                            SELECT 
                                'debug_test_' || ROW_NUMBER() OVER (ORDER BY 1) as id,
                                'test' as type,
                                'debug' as subtype,
                                'debug_insertion' as source_name
                            FROM google_scraper.{best_table_name}
                            LIMIT 3
                            ON CONFLICT (id) DO NOTHING
                        """)
                        rows_inserted = cursor.rowcount
                        conn.commit()
                        print(f"  ✅ Successfully inserted {rows_inserted} test rows")
                        
                        # Check if they're there
                        cursor.execute("SELECT COUNT(*) FROM google_scraper.google_scraper_eventos WHERE source_name = 'debug_insertion'")
                        debug_count = cursor.fetchone()[0]
                        print(f"  ✅ Found {debug_count} debug rows in eventos table")
                        
                    except Exception as e:
                        print(f"  ❌ Test insertion failed: {str(e)}")
                
                # 6. Provide recommendations
                print("\n📋 Step 6: Recommendations")
                if potential_source_tables:
                    best_table_name = potential_source_tables[0][0]
                    print(f"✅ Use table: google_scraper.{best_table_name}")
                    print("🔧 Fix the DATE issue by:")
                    print("   1. Remove the 'WHERE af.date IS NOT NULL' filter, OR")
                    print("   2. Use a different date column, OR") 
                    print("   3. Allow NULL dates in the insertion")
                else:
                    print("❌ No suitable source table found!")
                    print("🔧 You need to:")
                    print("   1. Upload your CSV data to a table in google_scraper schema")
                    print("   2. Make sure it has columns like ID, date, type, district, etc.")
                
    except ImportError:
        print("❌ psycopg2 not available")
    except Exception as e:
        print(f"❌ Debug failed: {str(e)}")

# Create the debug task
debug_task = PythonOperator(
    task_id='debug_empty_table_issue',
    python_callable=debug_table_issue,
    dag=dag,
)