#!/usr/bin/env python3
"""
Debug Coordinates DAG - Check why lat/lng is missing
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
    'debug_coordinates',
    default_args=default_args,
    description='Debug why coordinates are missing',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['debug', 'coordinates', 'simprede'],
    max_active_runs=1,
)

def debug_coordinates_issue(**context):
    """Debug why coordinates are missing from eventos table"""
    print("🔍 Debugging missing coordinates issue...")
    
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
                
                # 1. Check eventos table coordinate status
                print("📋 Step 1: Check eventos table coordinate status")
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_eventos,
                        COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coords,
                        COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as without_coords
                    FROM google_scraper.google_scraper_eventos
                """)
                stats = cursor.fetchone()
                total, with_coords, without_coords = stats
                print(f"  - Total eventos: {total}")
                print(f"  - With coordinates: {with_coords}")
                print(f"  - Without coordinates: {without_coords}")
                
                # 2. Check if centroids table exists
                print("\n📋 Step 2: Check centroids table")
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = 'centroids'
                """)
                centroids_exists = cursor.fetchone()[0]
                
                if centroids_exists:
                    print("  ✅ public.centroids table exists")
                    
                    # Check centroids data
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_centroids,
                            COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as valid_coords,
                            COUNT(DISTINCT freguesia) as freguesias,
                            COUNT(DISTINCT concelho) as concelhos,
                            COUNT(DISTINCT distrito) as distritos
                        FROM public.centroids
                    """)
                    centroids_stats = cursor.fetchone()
                    total_c, valid_c, freguesias, concelhos, distritos = centroids_stats
                    print(f"    • Total centroids: {total_c}")
                    print(f"    • With valid coordinates: {valid_c}")
                    print(f"    • Unique freguesias: {freguesias}")
                    print(f"    • Unique concelhos: {concelhos}")
                    print(f"    • Unique distritos: {distritos}")
                    
                else:
                    print("  ❌ public.centroids table NOT FOUND!")
                    print("    This is why coordinates are missing!")
                    return
                
                # 3. Sample location matching test
                print("\n📋 Step 3: Sample location matching test")
                cursor.execute("""
                    SELECT district, municipality, parish
                    FROM google_scraper.google_scraper_eventos 
                    WHERE latitude IS NULL
                    LIMIT 5
                """)
                sample_eventos = cursor.fetchall()
                
                for i, (district, municipality, parish) in enumerate(sample_eventos, 1):
                    print(f"  - Sample Event {i}:")
                    print(f"    District: '{district}'")
                    print(f"    Municipality: '{municipality}'") 
                    print(f"    Parish: '{parish}'")
                    
                    # Test matching with centroids
                    cursor.execute("""
                        SELECT COUNT(*) FROM public.centroids 
                        WHERE LOWER(TRIM(freguesia)) = LOWER(TRIM(%s))
                        AND latitude IS NOT NULL AND longitude IS NOT NULL
                    """, (parish,))
                    freguesia_matches = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT COUNT(*) FROM public.centroids 
                        WHERE LOWER(TRIM(concelho)) = LOWER(TRIM(%s))
                        AND latitude IS NOT NULL AND longitude IS NOT NULL
                    """, (municipality,))
                    concelho_matches = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT COUNT(*) FROM public.centroids 
                        WHERE LOWER(TRIM(distrito)) = LOWER(TRIM(%s))
                        AND latitude IS NOT NULL AND longitude IS NOT NULL
                    """, (district,))
                    distrito_matches = cursor.fetchone()[0]
                    
                    print(f"    Freguesia matches: {freguesia_matches}")
                    print(f"    Concelho matches: {concelho_matches}")
                    print(f"    Distrito matches: {distrito_matches}")
                    
                    if freguesia_matches == 0 and concelho_matches == 0 and distrito_matches == 0:
                        print("    ⚠️ NO MATCHES FOUND!")
                        
                        # Show similar names
                        cursor.execute("""
                            SELECT DISTINCT freguesia FROM public.centroids 
                            WHERE freguesia ILIKE %s OR freguesia ILIKE %s
                            LIMIT 3
                        """, (f'%{parish[:3]}%', f'%{parish[-3:]}%'))
                        similar_freguesias = cursor.fetchall()
                        if similar_freguesias:
                            print(f"    Similar freguesias: {[f[0] for f in similar_freguesias]}")
                    else:
                        print("    ✅ Matches found - should have coordinates")
                
                # 4. Test manual coordinate update
                print("\n📋 Step 4: Test manual coordinate update")
                cursor.execute("""
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
                """)
                
                rows_updated = cursor.rowcount
                conn.commit()
                print(f"  ✅ Updated {rows_updated} events with coordinates")
                
                # 5. Final status check
                print("\n📋 Step 5: Final coordinate status")
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_eventos,
                        COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coords,
                        COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as without_coords
                    FROM google_scraper.google_scraper_eventos
                """)
                final_stats = cursor.fetchone()
                final_total, final_with_coords, final_without_coords = final_stats
                print(f"  - Total eventos: {final_total}")
                print(f"  - With coordinates: {final_with_coords}")
                print(f"  - Without coordinates: {final_without_coords}")
                
                if final_with_coords > with_coords:
                    print(f"  🎉 SUCCESS! Added coordinates to {final_with_coords - with_coords} events")
                else:
                    print("  ⚠️ No coordinates were added - location names don't match centroids")
                
                # 6. Show sample of events with coordinates
                if final_with_coords > 0:
                    print("\n📋 Sample events with coordinates:")
                    cursor.execute("""
                        SELECT district, municipality, parish, latitude, longitude, georef_class
                        FROM google_scraper.google_scraper_eventos 
                        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                        LIMIT 5
                    """)
                    coord_samples = cursor.fetchall()
                    for district, municipality, parish, lat, lng, georef_class in coord_samples:
                        print(f"  - {district}/{municipality}/{parish}: ({lat}, {lng}) [{georef_class}]")
                
    except ImportError:
        print("❌ psycopg2 not available")
    except Exception as e:
        print(f"❌ Debug failed: {str(e)}")

# Create the debug task
debug_coords_task = PythonOperator(
    task_id='debug_coordinates_issue',
    python_callable=debug_coordinates_issue,
    dag=dag,
)