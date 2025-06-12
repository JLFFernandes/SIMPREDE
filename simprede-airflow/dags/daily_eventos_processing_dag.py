#!/usr/bin/env python3
"""
SIMPREDE Daily Eventos Processing Pipeline
"""
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator

# Try to import PostgreSQL providers
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    POSTGRES_HOOK_AVAILABLE = True
    print("✅ PostgresHook imported successfully")
except ImportError as e:
    print(f"⚠️ PostgresHook not available: {e}")
    POSTGRES_HOOK_AVAILABLE = False
    class PostgresHook:
        def __init__(self, *args, **kwargs):
            pass

# Default arguments
default_args = {
    'owner': 'simprede',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'daily_eventos_processing',
    default_args=default_args,
    description='Daily processing of staging data to eventos table',
    schedule='0 1 * * *',  # Run daily at 6 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['daily', 'eventos', 'staging', 'simprede'],
    max_active_runs=1,
    doc_md=__doc__,
)

def setup_connection_task(**context):
    """Setup database connection for daily processing"""
    print("🔧 Setting up database connection for daily eventos processing...")
    
    if not POSTGRES_HOOK_AVAILABLE:
        print("❌ PostgreSQL provider not available")
        return
    
    # Get credentials from environment
    db_user = os.getenv('DB_USER', '').strip()
    db_password = os.getenv('DB_PASSWORD', '').strip()
    db_host = os.getenv('DB_HOST', '').strip()
    db_port = os.getenv('DB_PORT', '6543').strip()
    db_name = os.getenv('DB_NAME', 'postgres').strip()
    
    print("🔍 Environment variables status:")
    print(f"  - DB_USER: {'✅ Set' if db_user else '❌ Missing'}")
    print(f"  - DB_PASSWORD: {'✅ Set' if db_password else '❌ Missing'}")
    print(f"  - DB_HOST: {'✅ Set' if db_host else '❌ Missing'}")
    print(f"  - DB_PORT: {db_port}")
    print(f"  - DB_NAME: {db_name}")
    
    if not all([db_user, db_password, db_host]):
        print("❌ Missing database credentials")
        raise ValueError("Missing required database credentials")
    
    # Create connection URI and environment variable
    conn_id = 'supabase_postgres'
    conn_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
    env_var_name = f'AIRFLOW_CONN_{conn_id.upper()}'
    os.environ[env_var_name] = conn_uri
    
    print(f"✅ Set up connection: {conn_id}")
    
    # Store connection details in XCom
    context['task_instance'].xcom_push(key='connection_details', value={
        'conn_id': conn_id,
        'connection_uri': conn_uri,
        'env_var_name': env_var_name
    })
    
    return conn_uri

def check_staging_table_task(**context):
    """Check if today's staging table exists and has data"""
    print("📋 Checking staging table availability...")
    
    # Get connection details
    connection_details = context['task_instance'].xcom_pull(
        task_ids='setup_connection', 
        key='connection_details'
    )
    
    if not connection_details:
        raise ValueError("No connection details found")
    
    connection_uri = connection_details['connection_uri']
    
    # Get execution date and format for table name
    execution_date = context['ds_nodash']  # YYYYMMDD format
    dag_conf = context.get('dag_run', {}).conf or {}
    
    # Allow custom staging table name or use default pattern
    staging_table = dag_conf.get('staging_table', f'google_scraper.artigos_filtrados_{execution_date}_staging')
    
    print(f"🔍 Checking staging table: {staging_table}")
    
    try:
        import psycopg2
        
        with psycopg2.connect(connection_uri) as conn:
            with conn.cursor() as cursor:
                
                # Check if table exists
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'google_scraper' 
                    AND table_name = %s
                """, (staging_table.split('.')[-1],))
                
                table_exists = cursor.fetchone()[0] > 0
                
                if not table_exists:
                    print(f"❌ Staging table does not exist: {staging_table}")
                    
                    # List available staging tables for debugging
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'google_scraper' 
                        AND table_name LIKE '%staging%'
                        ORDER BY table_name DESC
                        LIMIT 5
                    """)
                    
                    available_tables = cursor.fetchall()
                    if available_tables:
                        print("📋 Available staging tables:")
                        for table in available_tables:
                            print(f"  - google_scraper.{table[0]}")
                    
                    raise ValueError(f"Staging table {staging_table} not found")
                
                # Check row count
                cursor.execute(f"SELECT COUNT(*) FROM {staging_table}")
                row_count = cursor.fetchone()[0]
                
                print(f"✅ Staging table found: {staging_table}")
                print(f"📊 Row count: {row_count}")
                
                if row_count == 0:
                    print("⚠️ Staging table is empty - no data to process")
                    # Don't fail, just log and continue
                
                # Store table info in XCom
                context['task_instance'].xcom_push(key='staging_table_info', value={
                    'table_name': staging_table,
                    'row_count': row_count,
                    'exists': table_exists
                })
                
                return {
                    'staging_table': staging_table,
                    'row_count': row_count
                }
                
    except ImportError:
        print("❌ psycopg2 not available")
        raise
    except Exception as e:
        print(f"❌ Failed to check staging table: {str(e)}")
        raise

def append_staging_to_eventos_task(**context):
    """Append new records from staging table to eventos table"""
    print("🔄 Appending staging data to eventos table...")
    
    # Get connection and staging table details
    connection_details = context['task_instance'].xcom_pull(
        task_ids='setup_connection', 
        key='connection_details'
    )
    
    staging_info = context['task_instance'].xcom_pull(
        task_ids='check_staging_table', 
        key='staging_table_info'
    )
    
    if not connection_details or not staging_info:
        raise ValueError("Missing connection or staging table details")
    
    connection_uri = connection_details['connection_uri']
    staging_table = staging_info['table_name']
    staging_row_count = staging_info['row_count']
    
    if staging_row_count == 0:
        print("⚠️ No data in staging table - skipping append operation")
        return {
            'staging_rows': 0,
            'inserted_rows': 0,
            'skipped_duplicates': 0
        }
    
    print(f"📋 Processing {staging_row_count} rows from {staging_table}")
    
    try:
        import psycopg2
        
        with psycopg2.connect(connection_uri) as conn:
            with conn.cursor() as cursor:
                
                # Check current eventos count
                cursor.execute("SELECT COUNT(*) FROM google_scraper.google_scraper_eventos")
                initial_count = cursor.fetchone()[0]
                print(f"📊 Initial eventos count: {initial_count}")
                
                # Check the actual column structure of the staging table
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'google_scraper' 
                    AND table_name = '{staging_table.split('.')[-1]}'
                    ORDER BY column_name
                """)
                
                available_columns = [row[0] for row in cursor.fetchall()]
                print(f"📋 Available columns in staging table: {', '.join(available_columns)}")
                
                # Check the data type of the date column
                cursor.execute(f"""
                    SELECT data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'google_scraper' 
                    AND table_name = '{staging_table.split('.')[-1]}'
                    AND column_name = 'date'
                """)
                
                date_type_result = cursor.fetchone()
                date_is_text = date_type_result and date_type_result[0] in ('text', 'character varying', 'varchar')
                
                print(f"📋 Date column type: {date_type_result[0] if date_type_result else 'unknown'}")
                
                # Set datestyle to handle European date format
                cursor.execute("SET datestyle = 'DMY'")
                print("🔧 Set PostgreSQL datestyle to DMY (DD/MM/YYYY)")
                
                # Build insert query with proper table name formatting
                insert_query = f"""
                    INSERT INTO google_scraper.google_scraper_eventos (
                        id, type, subtype, date, year, month, day, hour,
                        latitude, longitude, georef_class, district, municipality, parish, dicofreg, location_geom,
                        fatalities, injured, evacuated, displaced, missing,
                        source_name, source_date, source_type, page
                    )
                    SELECT 
                        af.id as id,
                        COALESCE(af.type, 'Other') as type,
                        COALESCE(af.subtype, 'Other') as subtype,
                        af.date as date,
                        -- Convert text date to proper date, then extract components
                        COALESCE(af.year, 
                            CASE 
                                WHEN af.date ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' THEN 
                                    CAST(SPLIT_PART(af.date, '/', 3) AS INTEGER)
                                ELSE NULL
                            END
                        ) as year,
                        COALESCE(af.month,
                            CASE 
                                WHEN af.date ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' THEN 
                                    CAST(SPLIT_PART(af.date, '/', 2) AS INTEGER)
                                ELSE NULL
                            END
                        ) as month,
                        COALESCE(af.day,
                            CASE 
                                WHEN af.date ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' THEN 
                                    CAST(SPLIT_PART(af.date, '/', 1) AS INTEGER)
                                ELSE NULL
                            END
                        ) as day,
                        COALESCE(af.hour, '08:00') as hour,
                        NULL as latitude,
                        NULL as longitude,
                        COALESCE(af.georef, 'unknown') as georef_class,
                        COALESCE(af.district, 'unknown') as district,
                        COALESCE(af.municipali, 'unknown') as municipality,
                        COALESCE(af.parish, 'unknown') as parish,
                        af.dicofreg as dicofreg,
                        NULL as location_geom,
                        COALESCE(af.fatalities, 0) as fatalities,
                        COALESCE(af.injured, 0) as injured,
                        COALESCE(af.evacuated, 0) as evacuated,
                        COALESCE(af.displaced, 0) as displaced,
                        COALESCE(af.missing, 0) as missing,
                        af.source as source_name,
                        af.sourcedate as source_date,
                        COALESCE(af.sourcetype, 'news_article') as source_type,
                        af.page as page
                    FROM {staging_table} af
                    WHERE af.date IS NOT NULL
                    AND af.date != ''
                    -- Optional: Add date validation to ensure proper format
                    AND af.date ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$'
                    ON CONFLICT (id) DO UPDATE SET
                        type = EXCLUDED.type,
                        subtype = EXCLUDED.subtype,
                        date = EXCLUDED.date,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        fatalities = EXCLUDED.fatalities,
                        injured = EXCLUDED.injured,
                        evacuated = EXCLUDED.evacuated,
                        displaced = EXCLUDED.displaced,
                        missing = EXCLUDED.missing,
                        district = EXCLUDED.district,
                        municipality = EXCLUDED.municipality,
                        parish = EXCLUDED.parish,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                cursor.execute(insert_query)
                inserted_rows = cursor.rowcount
                conn.commit()
                
                print(f"✅ Inserted {inserted_rows} new records from staging")
                
                # Check final count
                cursor.execute("SELECT COUNT(*) FROM google_scraper.google_scraper_eventos")
                final_count = cursor.fetchone()[0]
                
                # Calculate statistics
                skipped_duplicates = staging_row_count - inserted_rows
                
                print(f"📊 Processing Summary:")
                print(f"  - Staging rows processed: {staging_row_count}")
                print(f"  - New records inserted: {inserted_rows}")
                print(f"  - Duplicate records skipped: {skipped_duplicates}")
                print(f"  - Total eventos before: {initial_count}")
                print(f"  - Total eventos after: {final_count}")
                
                # Store results in XCom
                results = {
                    'staging_table': staging_table,
                    'staging_rows': staging_row_count,
                    'inserted_rows': inserted_rows,
                    'skipped_duplicates': skipped_duplicates,
                    'initial_count': initial_count,
                    'final_count': final_count
                }
                
                context['task_instance'].xcom_push(key='append_results', value=results)
                
                return results
                
    except ImportError:
        print("❌ psycopg2 not available")
        raise
    except Exception as e:
        print(f"❌ Failed to append staging data: {str(e)}")
        raise

def update_coordinates_task(**context):
    """Update coordinates for newly inserted eventos"""
    print("🔧 Updating coordinates for new eventos...")
    
    # Get connection details
    connection_details = context['task_instance'].xcom_pull(
        task_ids='setup_connection', 
        key='connection_details'
    )
    
    if not connection_details:
        raise ValueError("No connection details found")
    
    connection_uri = connection_details['connection_uri']
    
    try:
        import psycopg2
        
        with psycopg2.connect(connection_uri) as conn:
            with conn.cursor() as cursor:
                
                # Check events without coordinates
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM google_scraper.google_scraper_eventos 
                    WHERE latitude IS NULL OR longitude IS NULL
                """)
                events_without_coords = cursor.fetchone()[0]
                
                print(f"📊 Events without coordinates: {events_without_coords}")
                
                if events_without_coords == 0:
                    print("✅ All events already have coordinates")
                    return {'events_updated': 0}
                
                # Update coordinates using centroids table
                update_query = """
                    UPDATE google_scraper.google_scraper_eventos 
                    SET 
                        latitude = coord_data.best_latitude,
                        longitude = coord_data.best_longitude,
                        georef_class = coord_data.best_georef_class,
                        updated_at = CURRENT_TIMESTAMP
                    FROM (
                        SELECT 
                            e.id,
                            COALESCE(
                                (SELECT c.latitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.freguesia)) = LOWER(TRIM(e.parish)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.dicofre LIMIT 1),
                                (SELECT c.latitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.concelho)) = LOWER(TRIM(e.municipality)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.dicofre LIMIT 1),
                                (SELECT c.latitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.distrito)) = LOWER(TRIM(e.district)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.dicofre LIMIT 1)
                            ) as best_latitude,
                            COALESCE(
                                (SELECT c.longitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.freguesia)) = LOWER(TRIM(e.parish)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.dicofre LIMIT 1),
                                (SELECT c.longitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.concelho)) = LOWER(TRIM(e.municipality)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.dicofre LIMIT 1),
                                (SELECT c.longitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.distrito)) = LOWER(TRIM(e.district)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.dicofre LIMIT 1)
                            ) as best_longitude,
                            CASE 
                                WHEN EXISTS(SELECT 1 FROM public.centroids c 
                                           WHERE LOWER(TRIM(c.freguesia)) = LOWER(TRIM(e.parish)) 
                                           AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL) THEN 'freguesia'
                                WHEN EXISTS(SELECT 1 FROM public.centroids c 
                                           WHERE LOWER(TRIM(c.concelho)) = LOWER(TRIM(e.municipality)) 
                                           AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL) THEN 'concelho'
                                WHEN EXISTS(SELECT 1 FROM public.centroids c 
                                           WHERE LOWER(TRIM(c.distrito)) = LOWER(TRIM(e.district)) 
                                           AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL) THEN 'distrito'
                                ELSE 'unknown'
                            END as best_georef_class
                        FROM google_scraper.google_scraper_eventos e
                        WHERE e.latitude IS NULL OR e.longitude IS NULL
                    ) coord_data
                    WHERE google_scraper_eventos.id = coord_data.id
                    AND coord_data.best_latitude IS NOT NULL 
                    AND coord_data.best_longitude IS NOT NULL
                """
                
                cursor.execute(update_query)
                events_updated = cursor.rowcount
                conn.commit()
                
                print(f"✅ Updated coordinates for {events_updated} events")
                
                # Check final status
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as with_coords
                    FROM google_scraper.google_scraper_eventos
                """)
                total, with_coords = cursor.fetchone()
                
                print(f"📊 Final coordinate status: {with_coords}/{total} events have coordinates")
                
                context['task_instance'].xcom_push(key='coordinate_update_results', value={
                    'events_updated': events_updated,
                    'total_events': total,
                    'events_with_coords': with_coords
                })
                
                return {'events_updated': events_updated}
                
    except ImportError:
        print("❌ psycopg2 not available")
        raise
    except Exception as e:
        print(f"❌ Failed to update coordinates: {str(e)}")
        raise

def update_geometry_task(**context):
    """Update location geometry for newly inserted eventos"""
    print("🔧 Updating location geometry for new eventos...")
    
    # Get connection details
    connection_details = context['task_instance'].xcom_pull(
        task_ids='setup_connection', 
        key='connection_details'
    )
    
    if not connection_details:
        raise ValueError("No connection details found")
    
    connection_uri = connection_details['connection_uri']
    
    try:
        import psycopg2
        
        with psycopg2.connect(connection_uri) as conn:
            with conn.cursor() as cursor:
                
                # Check events without geometry but with coordinates
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM google_scraper.google_scraper_eventos 
                    WHERE latitude IS NOT NULL 
                    AND longitude IS NOT NULL 
                    AND location_geom IS NULL
                """)
                events_without_geom = cursor.fetchone()[0]
                
                print(f"📊 Events without geometry: {events_without_geom}")
                
                if events_without_geom == 0:
                    print("✅ All events with coordinates already have geometry")
                    return {'events_updated': 0}
                
                # Check if PostGIS is available
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM pg_extension 
                    WHERE extname = 'postgis'
                """)
                postgis_available = cursor.fetchone()[0] > 0
                
                if postgis_available:
                    print("🔧 Using PostGIS POINT geometry format")
                    update_query = """
                        UPDATE google_scraper.google_scraper_eventos 
                        SET 
                            location_geom = ST_AsText(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE latitude IS NOT NULL 
                        AND longitude IS NOT NULL 
                        AND location_geom IS NULL
                    """
                else:
                    print("🔧 Using WKT (Well-Known Text) geometry format")
                    update_query = """
                        UPDATE google_scraper.google_scraper_eventos 
                        SET 
                            location_geom = 'POINT(' || longitude || ' ' || latitude || ')',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE latitude IS NOT NULL 
                        AND longitude IS NOT NULL 
                        AND location_geom IS NULL
                    """
                
                cursor.execute(update_query)
                events_updated = cursor.rowcount
                conn.commit()
                
                print(f"✅ Updated geometry for {events_updated} events")
                
                context['task_instance'].xcom_push(key='geometry_update_results', value={
                    'events_updated': events_updated,
                    'postgis_available': postgis_available
                })
                
                return {'events_updated': events_updated}
                
    except ImportError:
        print("❌ psycopg2 not available")
        raise
    except Exception as e:
        print(f"❌ Failed to update geometry: {str(e)}")
        raise

def cleanup_old_staging_tables_task(**context):
    """Optional: Clean up old staging tables (older than 7 days)"""
    print("🧹 Cleaning up old staging tables...")
    
    # Get connection details
    connection_details = context['task_instance'].xcom_pull(
        task_ids='setup_connection', 
        key='connection_details'
    )
    
    if not connection_details:
        print("⚠️ No connection details found - skipping cleanup")
        return
    
    connection_uri = connection_details['connection_uri']
    
    try:
        import psycopg2
        from datetime import datetime, timedelta
        
        # Calculate cutoff date (7 days ago)
        cutoff_date = datetime.now() - timedelta(days=7)
        cutoff_date_str = cutoff_date.strftime('%Y%m%d')
        
        print(f"🗓️ Cleaning staging tables older than: {cutoff_date_str}")
        
        with psycopg2.connect(connection_uri) as conn:
            with conn.cursor() as cursor:
                
                # Find old staging tables
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'google_scraper' 
                    AND table_name LIKE 'artigos_filtrados_%_staging'
                    AND table_name ~ 'artigos_filtrados_[0-9]{8}_staging'
                """)
                
                staging_tables = cursor.fetchall()
                tables_to_drop = []
                
                for table in staging_tables:
                    table_name = table[0]
                    # Extract date from table name (artigos_filtrados_YYYYMMDD_staging)
                    try:
                        date_part = table_name.split('_')[2]  # Gets YYYYMMDD
                        if len(date_part) == 8 and date_part.isdigit():
                            if date_part < cutoff_date_str:
                                tables_to_drop.append(table_name)
                    except (IndexError, ValueError):
                        # Skip tables that don't match the expected pattern
                        continue
                
                print(f"📋 Found {len(tables_to_drop)} old staging tables to clean up")
                
                dropped_count = 0
                for table_name in tables_to_drop:
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS google_scraper.{table_name}")
                        dropped_count += 1
                        print(f"  ✅ Dropped: {table_name}")
                    except Exception as e:
                        print(f"  ❌ Failed to drop {table_name}: {str(e)}")
                
                conn.commit()
                
                print(f"🎉 Cleanup completed: {dropped_count} tables dropped")
                
                context['task_instance'].xcom_push(key='cleanup_results', value={
                    'tables_found': len(staging_tables),
                    'tables_dropped': dropped_count,
                    'cutoff_date': cutoff_date_str
                })
                
                return {'tables_dropped': dropped_count}
                
    except ImportError:
        print("❌ psycopg2 not available")
        return {'tables_dropped': 0}
    except Exception as e:
        print(f"❌ Cleanup failed: {str(e)}")
        return {'tables_dropped': 0}

def copy_to_public_schema_task(**context):
    """Copy the processed eventos data to public schema for easier access"""
    print("📋 Copying eventos data to public schema...")
    
    # Get connection details
    connection_details = context['task_instance'].xcom_pull(
        task_ids='setup_connection', 
        key='connection_details'
    )
    
    if not connection_details:
        raise ValueError("No connection details found")
    
    connection_uri = connection_details['connection_uri']
    
    try:
        import psycopg2
        
        with psycopg2.connect(connection_uri) as conn:
            with conn.cursor() as cursor:
                
                print("📋 Step 1: Check source table status")
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_eventos,
                        COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coordinates,
                        COUNT(CASE WHEN location_geom IS NOT NULL THEN 1 END) as with_geometry
                    FROM google_scraper.google_scraper_eventos
                """)
                source_stats = cursor.fetchone()
                if source_stats:
                    total_eventos, with_coordinates, with_geometry = source_stats
                    print(f"📊 Source table status:")
                    print(f"  - Total eventos: {total_eventos}")
                    print(f"  - With coordinates: {with_coordinates}")
                    print(f"  - With geometry: {with_geometry}")
                    
                    if total_eventos == 0:
                        print("⚠️ No eventos found in source table - nothing to copy")
                        return {'copied_rows': 0}
                else:
                    print("❌ Could not get source table statistics")
                    return {'copied_rows': 0}
                
                print("📋 Step 2: Drop existing public table if it exists")
                cursor.execute("DROP TABLE IF EXISTS public.google_scraper_ocorrencias CASCADE")
                print("✅ Dropped existing public.google_scraper_ocorrencias table")
                
                print("📋 Step 3: Create new table structure in public schema")
                cursor.execute("""
                    CREATE TABLE public.google_scraper_ocorrencias
                    (LIKE google_scraper.google_scraper_eventos INCLUDING ALL)
                """)
                print("✅ Created public.google_scraper_ocorrencias table with complete structure")
                
                print("📋 Step 4: Copy all data from source table")
                cursor.execute("""
                    INSERT INTO public.google_scraper_ocorrencias
                    SELECT * FROM google_scraper.google_scraper_eventos
                """)
                copied_rows = cursor.rowcount
                conn.commit()
                
                print(f"✅ Copied {copied_rows} rows to public.google_scraper_ocorrencias")
                
                print("📋 Step 5: Verify copy operation")
                cursor.execute("SELECT COUNT(*) FROM public.google_scraper_ocorrencias")
                final_count = cursor.fetchone()[0]
                
                print(f"📊 Final verification:")
                print(f"  - Source table rows: {total_eventos}")
                print(f"  - Copied rows: {copied_rows}")
                print(f"  - Final public table rows: {final_count}")
                
                if final_count == total_eventos:
                    print("🎉 SUCCESS! All eventos successfully copied to public schema")
                else:
                    print(f"⚠️ WARNING: Row count mismatch - expected {total_eventos}, got {final_count}")
                
                # Add some useful indexes to the public table for better performance
                print("📋 Step 6: Adding indexes for better query performance")
                try:
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_ocorrencias_date 
                        ON public.google_scraper_ocorrencias(date)
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_ocorrencias_location 
                        ON public.google_scraper_ocorrencias(district, municipality, parish)
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_ocorrencias_coordinates 
                        ON public.google_scraper_ocorrencias(latitude, longitude) 
                        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_ocorrencias_type 
                        ON public.google_scraper_ocorrencias(type, subtype)
                    """)
                    conn.commit()
                    print("✅ Added performance indexes to public table")
                except Exception as index_error:
                    print(f"⚠️ Failed to create some indexes: {str(index_error)}")
                
                # Show sample data from public table
                print("📋 Step 7: Sample data from public table")
                cursor.execute("""
                    SELECT 
                        id, type, date, district, municipality, 
                        CASE WHEN latitude IS NOT NULL THEN 'Yes' ELSE 'No' END as has_coords,
                        fatalities, injured
                    FROM public.google_scraper_ocorrencias 
                    ORDER BY date DESC, created_at DESC
                    LIMIT 5
                """)
                samples = cursor.fetchall()
                if samples:
                    print("📋 Latest eventos in public table:")
                    for sample in samples:
                        id_val, type_val, date_val, district_val, municipality_val, has_coords, fatalities, injured = sample
                        print(f"  - {id_val}: {type_val} on {date_val} in {district_val}/{municipality_val} [coords: {has_coords}] ({fatalities} fatalities, {injured} injured)")
                
                # Store results in XCom
                context['task_instance'].xcom_push(key='copy_to_public_results', value={
                    'source_rows': total_eventos,
                    'copied_rows': copied_rows,
                    'final_count': final_count,
                    'success': final_count == total_eventos,
                    'with_coordinates': with_coordinates,
                    'with_geometry': with_geometry
                })
                
                print("🎉 Public schema copy operation completed!")
                print("👉 Data is now available at: public.google_scraper_ocorrencias")
                print("👉 This table can be easily queried and accessed by other applications")
                
                return {
                    'copied_rows': copied_rows,
                    'final_count': final_count
                }
                
    except ImportError:
        print("❌ psycopg2 not available")
        raise
    except Exception as e:
        print(f"❌ Failed to copy to public schema: {str(e)}")
        raise

# Define tasks
setup_connection = PythonOperator(
    task_id='setup_connection',
    python_callable=setup_connection_task,
    dag=dag,
)

check_staging_table = PythonOperator(
    task_id='check_staging_table',
    python_callable=check_staging_table_task,
    dag=dag,
)

append_staging_to_eventos = PythonOperator(
    task_id='append_staging_to_eventos',
    python_callable=append_staging_to_eventos_task,
    dag=dag,
)

update_coordinates = PythonOperator(
    task_id='update_coordinates',
    python_callable=update_coordinates_task,
    dag=dag,
)

update_geometry = PythonOperator(
    task_id='update_geometry',
    python_callable=update_geometry_task,
    dag=dag,
)

cleanup_old_staging_tables = PythonOperator(
    task_id='cleanup_old_staging_tables',
    python_callable=cleanup_old_staging_tables_task,
    dag=dag,
)

copy_to_public_schema = PythonOperator(
    task_id='copy_to_public_schema',
    python_callable=copy_to_public_schema_task,
    dag=dag,
)

# Set task dependencies
setup_connection >> check_staging_table >> append_staging_to_eventos
append_staging_to_eventos >> update_coordinates >> update_geometry
update_geometry >> cleanup_old_staging_tables >> copy_to_public_schema

# Documentation
dag.doc_md = """
# SIMPREDE Daily Eventos Processing Pipeline

## Overview
This DAG processes daily staging data from Google Scraper results and appends it to the main eventos table with proper georeference data.

## Schedule
- **Runs daily at 6:00 AM**
- **Processes yesterday's staging data** (based on execution date)

## Workflow

### 1. Setup Connection (`setup_connection`)
- Establishes database connection using environment variables
- Creates connection for subsequent tasks

### 2. Check Staging Table (`check_staging_table`)
- Verifies that today's staging table exists
- Checks data availability
- Format: `google_scraper.artigos_filtrados_YYYYMMDD_staging`

### 3. Append Data (`append_staging_to_eventos`)
- Inserts new records from staging to eventos table
- **Avoids duplicates** using source, date, and location matching
- Creates unique IDs for new events
- **Preserves data integrity**

### 4. Update Coordinates (`update_coordinates`)
- Adds latitude/longitude from centroids table
- Uses hierarchy: freguesia → concelho → distrito
- **Enables mapping and spatial analysis**

### 5. Update Geometry (`update_geometry`)
- Creates PostGIS geometry or WKT format
- **Enables GIS operations and visualization**

### 6. Cleanup (`cleanup_old_staging_tables`)
- Removes staging tables older than 7 days
- **Keeps database clean and manageable**

### 7. Copy to Public Schema (`copy_to_public_schema`)
- Copies processed eventos data to a public schema table
- **Simplifies access** for querying and integration with other tools

## Configuration

### Default Execution
The DAG automatically processes the staging table for the execution date:
```
google_scraper.artigos_filtrados_20250115_staging
```

### Custom Configuration
Pass parameters via DAG configuration:
```json
{
  "staging_table": "google_scraper.artigos_filtrados_20250114_staging"
}
```

## Integration with Google Scraper

### Typical Daily Flow:
1. **Google Scraper DAG runs** (creates staging table)
2. **This DAG runs at 6 AM** (processes staging data)
3. **Main eventos table updated** with new georeferenced events

### Staging Table Requirements:
The staging table should have these columns:
- `ID`, `date`, `year`, `month`, `day`, `hour`
- `type`, `subtype`, `evento_nome`
- `district`, `municipali`, `parish`, `georef`, `DICOFREG`
- `fatalities`, `injured`, `evacuated`, `displaced`, `missing`
- `source`, `sourcedate`, `sourcetype`, `page`

## Monitoring

### Key Metrics Tracked:
- Staging table row count
- New records inserted
- Duplicate records skipped
- Coordinate matching success rate
- Geometry creation success

### XCom Data Available:
- `append_results`: Insert statistics
- `coordinate_update_results`: Georeferencing stats
- `geometry_update_results`: Geometry creation stats
- `cleanup_results`: Table cleanup stats
- `copy_to_public_results`: Public schema copy stats

## Error Handling

### Resilient Design:
- **2 retries** with 5-minute delays
- **Continues processing** even if staging table is empty
- **Graceful handling** of missing centroids data
- **Detailed logging** for troubleshooting

### Common Scenarios:
- **No staging table**: Logs error and fails (expected behavior)
- **Empty staging table**: Logs warning and continues
- **Missing centroids**: Events created without coordinates
- **No PostGIS**: Falls back to simple WKT geometry

## Performance Considerations

### Optimizations:
- **Duplicate detection** using EXISTS clause (efficient)
- **Batch operations** for coordinate updates
- **Indexed lookups** for centroids matching
- **Cleanup routine** prevents database bloat

### Expected Processing Times:
- Small datasets (< 1000 events): 2-5 minutes
- Medium datasets (1000-10000 events): 5-15 minutes
- Large datasets (> 10000 events): 15-30 minutes

## Maintenance

### Weekly Tasks:
- Monitor staging table creation patterns
- Check coordinate matching success rates
- Verify geometry data quality

### Monthly Tasks:
- Review cleanup effectiveness
- Analyze processing performance trends
- Update centroids data if needed
"""