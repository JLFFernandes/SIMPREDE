#!/usr/bin/env python3
"""
Fix Coordinates DAG - Add centroids data and update coordinates
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
    'fix_coordinates',
    default_args=default_args,
    description='Fix missing coordinates by adding centroids data',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['fix', 'coordinates', 'centroids', 'simprede'],
    max_active_runs=1,
)

def create_and_populate_centroids(**context):
    """Create centroids table and populate with Portuguese administrative data"""
    print("🔧 Creating and populating centroids table...")
    
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
                
                # 1. Create centroids table
                print("📋 Step 1: Creating centroids table")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS public.centroids (
                        id SERIAL PRIMARY KEY,
                        dicofre TEXT,
                        distrito TEXT,
                        concelho TEXT,
                        freguesia TEXT,
                        latitude DOUBLE PRECISION,
                        longitude DOUBLE PRECISION,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 2. Check if table is empty
                cursor.execute("SELECT COUNT(*) FROM public.centroids")
                existing_count = cursor.fetchone()[0]
                print(f"  - Existing centroids: {existing_count}")
                
                if existing_count == 0:
                    # 3. Insert sample Portuguese administrative centroids
                    print("📋 Step 2: Inserting Portuguese centroids data")
                    
                    # Basic centroids for major Portuguese locations mentioned in your events
                    centroids_data = [
                        # District, Municipality, Parish, Latitude, Longitude
                        ('Aveiro', 'Aveiro', 'Aveiro', 40.6443, -8.6455),
                        ('Aveiro', 'Águeda', 'Águeda', 40.5763, -8.4433),
                        ('Aveiro', 'Arouca', 'Arouca', 40.9319, -8.2442),
                        ('Aveiro', 'Espinho', 'Espinho', 41.0077, -8.6414),
                        ('Aveiro', 'Estarreja', 'Estarreja', 40.7581, -8.5717),
                        ('Aveiro', 'Mealhada', 'Mealhada', 40.3781, -8.4490),
                        ('Aveiro', 'Ovar', 'Ovar', 40.8603, -8.6264),
                        ('Aveiro', 'Anadia', 'Anadia', 40.4413, -8.4310),
                        
                        ('Braga', 'Braga', 'Braga', 41.5454, -8.4265),
                        ('Braga', 'Barcelos', 'Barcelos', 41.5388, -8.6151),
                        ('Braga', 'Guimarães', 'Guimarães', 41.4416, -8.2918),
                        ('Braga', 'Vila Verde', 'Vila Verde', 41.6458, -8.4384),
                        ('Braga', 'Amares', 'Amares', 41.6088, -8.3502),
                        
                        ('Porto', 'Porto', 'Porto', 41.1579, -8.6291),
                        ('Porto', 'Trofa', 'Trofa', 41.3389, -8.5605),
                        
                        ('Lisboa', 'Lisboa', 'Lisboa', 38.7223, -9.1393),
                        ('Lisboa', 'Torres Vedras', 'Torres Vedras', 39.0910, -9.2591),
                        ('Lisboa', 'Sintra', 'Sintra', 38.7979, -9.3872),
                        ('Lisboa', 'Oeiras', 'Oeiras', 38.6873, -9.3097),
                        ('Lisboa', 'Mafra', 'Mafra', 38.9366, -9.3266),
                        
                        ('Coimbra', 'Coimbra', 'Coimbra', 40.2033, -8.4103),
                        ('Coimbra', 'Cantanhede', 'Cantanhede', 40.3580, -8.5950),
                        ('Coimbra', 'Mira', 'Mira', 40.4597, -8.7397),
                        ('Coimbra', 'Arganil', 'Arganil', 40.2053, -8.0553),
                        
                        ('Leiria', 'Batalha', 'Batalha', 39.6598, -8.8249),
                        ('Leiria', 'Óbidos', 'Óbidos', 39.3606, -9.1571),
                        ('Leiria', 'Pedrógão Grande', 'Pedrógão Grande', 39.9170, -8.1450),
                        
                        ('Beja', 'Beja', 'Beja', 38.0150, -7.8653),
                        ('Beja', 'Moura', 'Moura', 38.1436, -7.4471),
                        ('Beja', 'Ourique', 'Ourique', 37.6433, -8.2090),
                        
                        ('Évora', 'Évora', 'Évora', 38.5667, -7.9067),
                        ('Évora', 'Estremoz', 'Estremoz', 38.8433, -7.5867),
                        ('Évora', 'Mourão', 'Mourão', 38.3933, -7.3267),
                        ('Évora', 'Borba', 'Borba', 38.8033, -7.4533),
                        ('Évora', 'Mora', 'Mora', 38.9433, -8.1633),
                        ('Évora', 'Portel', 'Portel', 38.3033, -7.7167),
                        
                        ('Faro', 'Faro', 'Faro', 37.0194, -7.9322),
                        ('Faro', 'Albufeira', 'Albufeira', 37.0887, -8.2508),
                        ('Faro', 'Silves', 'Silves', 37.1909, -8.4384),
                        ('Faro', 'Lagos', 'Lagos', 37.1020, -8.6729),
                        ('Faro', 'Portimão', 'Portimão', 37.1364, -8.5370),
                        
                        ('Guarda', 'Guarda', 'Guarda', 40.5364, -7.2683),
                        ('Guarda', 'Mêda', 'Mêda', 40.9686, -7.2506),
                        ('Guarda', 'Seia', 'Seia', 40.4176, -7.7006),
                        
                        ('Castelo Branco', 'Castelo Branco', 'Castelo Branco', 39.8222, -7.4931),
                        ('Castelo Branco', 'Covilhã', 'Covilhã', 40.2756, -7.5036),
                        ('Castelo Branco', 'Oleiros', 'Oleiros', 39.9981, -7.9144),
                        ('Castelo Branco', 'Sertã', 'Sertã', 39.8031, -8.0995),
                        
                        ('Portalegre', 'Avis', 'Avis', 39.0267, -7.8933),
                        ('Portalegre', 'Gavião', 'Gavião', 39.4567, -7.9350),
                        ('Portalegre', 'Elvas', 'Elvas', 38.8811, -7.1631),
                        
                        ('Santarém', 'Mação', 'Mação', 39.5533, -8.0167),
                        
                        ('Bragança', 'Vila Flor', 'Vila Flor', 41.3067, -7.1533),
                    ]
                    
                    # Insert the data
                    insert_query = """
                        INSERT INTO public.centroids (distrito, concelho, freguesia, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    
                    cursor.executemany(insert_query, centroids_data)
                    rows_inserted = cursor.rowcount
                    conn.commit()
                    
                    print(f"  ✅ Inserted {rows_inserted} centroids records")
                else:
                    print("  ✅ Centroids table already has data")
                
                # 4. Verify data was inserted
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT distrito) as distritos,
                        COUNT(DISTINCT concelho) as concelhos,
                        COUNT(DISTINCT freguesia) as freguesias
                    FROM public.centroids
                """)
                stats = cursor.fetchone()
                total, distritos, concelhos, freguesias = stats
                print(f"  📊 Final centroids stats:")
                print(f"    • Total records: {total}")
                print(f"    • Unique distritos: {distritos}")
                print(f"    • Unique concelhos: {concelhos}")
                print(f"    • Unique freguesias: {freguesias}")
                
    except ImportError:
        print("❌ psycopg2 not available")
    except Exception as e:
        print(f"❌ Failed to create centroids: {str(e)}")

def update_eventos_coordinates(**context):
    """Update eventos table with coordinates from centroids"""
    print("🔧 Updating eventos with coordinates...")
    
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
                
                # Check initial status
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as with_coords
                    FROM google_scraper.google_scraper_eventos
                """)
                initial_stats = cursor.fetchone()
                initial_total, initial_with_coords = initial_stats
                print(f"📊 Initial status: {initial_with_coords}/{initial_total} events have coordinates")
                
                # Update coordinates with priority: freguesia > concelho > distrito
                update_query = """
                    UPDATE google_scraper.google_scraper_eventos 
                    SET 
                        latitude = coord_data.best_latitude,
                        longitude = coord_data.best_longitude,
                        georef_class = coord_data.best_georef_class,
                        dicofreg = coord_data.best_dicofre,
                        updated_at = CURRENT_TIMESTAMP
                    FROM (
                        SELECT 
                            e.id,
                            COALESCE(
                                (SELECT c.latitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.freguesia)) = LOWER(TRIM(e.parish)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.id LIMIT 1),
                                (SELECT c.latitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.concelho)) = LOWER(TRIM(e.municipality)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.id LIMIT 1),
                                (SELECT c.latitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.distrito)) = LOWER(TRIM(e.district)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.id LIMIT 1)
                            ) as best_latitude,
                            COALESCE(
                                (SELECT c.longitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.freguesia)) = LOWER(TRIM(e.parish)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.id LIMIT 1),
                                (SELECT c.longitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.concelho)) = LOWER(TRIM(e.municipality)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.id LIMIT 1),
                                (SELECT c.longitude FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.distrito)) = LOWER(TRIM(e.district)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.id LIMIT 1)
                            ) as best_longitude,
                            COALESCE(
                                (SELECT c.dicofre FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.freguesia)) = LOWER(TRIM(e.parish)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.id LIMIT 1),
                                (SELECT c.dicofre FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.concelho)) = LOWER(TRIM(e.municipality)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.id LIMIT 1),
                                (SELECT c.dicofre FROM public.centroids c 
                                 WHERE LOWER(TRIM(c.distrito)) = LOWER(TRIM(e.district)) 
                                 AND c.latitude IS NOT NULL AND c.longitude IS NOT NULL
                                 ORDER BY c.id LIMIT 1)
                            ) as best_dicofre,
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
                rows_updated = cursor.rowcount
                conn.commit()
                
                print(f"  ✅ Updated {rows_updated} events with coordinates")
                
                # Check final status
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as with_coords,
                        COUNT(CASE WHEN latitude IS NULL THEN 1 END) as without_coords
                    FROM google_scraper.google_scraper_eventos
                """)
                final_stats = cursor.fetchone()
                final_total, final_with_coords, final_without_coords = final_stats
                
                print(f"📊 Final status:")
                print(f"  - Total eventos: {final_total}")
                print(f"  - With coordinates: {final_with_coords}")
                print(f"  - Without coordinates: {final_without_coords}")
                print(f"  - Improvement: +{final_with_coords - initial_with_coords} coordinates added")
                
                # Show sample results
                if final_with_coords > 0:
                    print("\n📋 Sample events with coordinates:")
                    cursor.execute("""
                        SELECT district, municipality, parish, latitude, longitude, georef_class
                        FROM google_scraper.google_scraper_eventos 
                        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                        ORDER BY updated_at DESC
                        LIMIT 5
                    """)
                    samples = cursor.fetchall()
                    for district, municipality, parish, lat, lng, georef_class in samples:
                        print(f"  - {district}/{municipality}/{parish}: ({lat:.4f}, {lng:.4f}) [{georef_class}]")
                
    except ImportError:
        print("❌ psycopg2 not available")
    except Exception as e:
        print(f"❌ Failed to update coordinates: {str(e)}")

# Create tasks
create_centroids_task = PythonOperator(
    task_id='create_and_populate_centroids',
    python_callable=create_and_populate_centroids,
    dag=dag,
)

update_coordinates_task = PythonOperator(
    task_id='update_eventos_coordinates',
    python_callable=update_eventos_coordinates,
    dag=dag,
)

# Set dependencies
create_centroids_task >> update_coordinates_task