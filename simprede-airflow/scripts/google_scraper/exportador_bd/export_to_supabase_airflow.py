import sys
import os
import logging
import argparse
import pandas as pd
import psycopg2
import re
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# Set unbuffered output for Airflow compatibility
os.environ['PYTHONUNBUFFERED'] = '1'
try:
    # reconfigure method is only available in Python 3.7+
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True)  # type: ignore
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(line_buffering=True)  # type: ignore
except AttributeError:
    # Fallback for older Python versions
    import io
    sys.stdout = io.TextIOWrapper(open(sys.stdout.fileno(), 'wb', 0), write_through=True)
    sys.stderr = io.TextIOWrapper(open(sys.stderr.fileno(), 'wb', 0), write_through=True)

# Configure logging for Airflow compatibility
def setup_airflow_logging():
    """Setup logging that works well with Airflow UI"""
    class ImmediateFlushHandler(logging.StreamHandler):
        def emit(self, record):
            super().emit(record)
            self.flush()
            if hasattr(self.stream, 'flush'):
                self.stream.flush()
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("export_to_supabase_airflow")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    handler = ImmediateFlushHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    
    return logger

logger = setup_airflow_logging()

def log_progress(message, level="info", flush=True):
    """Log with guaranteed immediate visibility for Airflow"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"[{timestamp}] {message}"
    
    print(formatted_message, flush=True)
    
    if level == "warning":
        logger.warning(message)
    elif level == "error":
        logger.error(message)
    elif level == "debug":
        logger.debug(message)
    else:
        logger.info(message)
    
    if hasattr(sys.stdout, 'flush'):
        sys.stdout.flush()
    if hasattr(sys.stderr, 'flush'):
        sys.stderr.flush()

def get_database_config():
    """Get database configuration from environment variables or .env file"""
    # First try to get from environment variables (Airflow container)
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '6543'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'sslmode': os.getenv('DB_SSLMODE', 'require'),
        'schema': os.getenv('DB_SCHEMA', 'google_scraper')  # 🎯 DEFAULT SCHEMA: 'google_scraper'
    }
    
    log_progress("🔍 Initial environment check...")
    log_progress(f"  DB_HOST from env: {'FOUND' if db_config['host'] else 'NOT_FOUND'}")
    log_progress(f"  DB_USER from env: {'FOUND' if db_config['user'] else 'NOT_FOUND'}")
    log_progress(f"  DB_PASSWORD from env: {'FOUND' if db_config['password'] else 'NOT_FOUND'}")
    
    # If not found in environment, try to load from .env file
    if not db_config['host'] or not db_config['user'] or not db_config['password']:
        log_progress("🔍 Database config not found in environment, looking for .env file...")
        
        # Look for .env file in multiple locations
        possible_env_paths = [
            '/opt/airflow/.env',  # Airflow container root
            '/opt/airflow/scripts/google_scraper/.env',
            '/opt/airflow/scripts/.env',
            os.path.join(os.path.dirname(__file__), '../../../../.env'),
            os.path.join(os.path.dirname(__file__), '../../../.env'),
            os.path.join(os.path.dirname(__file__), '../../.env'),
            os.path.join(os.path.dirname(__file__), '../.env'),
            os.path.join(os.path.dirname(__file__), '.env')
        ]
        
        # Add absolute path resolution
        script_dir = os.path.dirname(os.path.abspath(__file__))
        additional_paths = [
            os.path.join(script_dir, '../../../../.env'),
            os.path.join(script_dir, '../../../.env'),
            '/Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow/.env'  # Absolute fallback
        ]
        possible_env_paths.extend(additional_paths)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_paths = []
        for path in possible_env_paths:
            abs_path = os.path.abspath(path)
            if abs_path not in seen:
                seen.add(abs_path)
                unique_paths.append(abs_path)
        
        env_file_found = False
        log_progress(f"🔍 Checking {len(unique_paths)} possible .env locations...")
        
        for env_path in unique_paths:
            log_progress(f"  Checking: {env_path}")
            if os.path.exists(env_path):
                log_progress(f"✅ Found .env file at: {env_path}")
                try:
                    # Simple .env parser with better error handling
                    with open(env_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        log_progress(f"📄 .env file size: {len(content)} chars")
                        
                        for line_num, line in enumerate(content.splitlines(), 1):
                            line = line.strip()
                            if line and not line.startswith('#') and '=' in line:
                                try:
                                    key, value = line.split('=', 1)
                                    key = key.strip()
                                    value = value.strip()
                                    
                                    # Remove quotes if present
                                    if value.startswith('"') and value.endswith('"'):
                                        value = value[1:-1]
                                    elif value.startswith("'") and value.endswith("'"):
                                        value = value[1:-1]
                                    
                                    if key == 'DB_HOST' and not db_config['host']:
                                        db_config['host'] = value
                                        log_progress(f"  ✅ Set DB_HOST from .env")
                                    elif key == 'DB_PORT':
                                        db_config['port'] = value
                                        log_progress(f"  ✅ Set DB_PORT from .env")
                                    elif key == 'DB_NAME':
                                        db_config['database'] = value
                                        log_progress(f"  ✅ Set DB_NAME from .env")
                                    elif key == 'DB_USER' and not db_config['user']:
                                        db_config['user'] = value
                                        log_progress(f"  ✅ Set DB_USER from .env")
                                    elif key == 'DB_PASSWORD' and not db_config['password']:
                                        db_config['password'] = value
                                        log_progress(f"  ✅ Set DB_PASSWORD from .env")
                                    elif key == 'DB_SSLMODE':
                                        db_config['sslmode'] = value
                                        log_progress(f"  ✅ Set DB_SSLMODE from .env")
                                    elif key == 'DB_SCHEMA':
                                        db_config['schema'] = value
                                        log_progress(f"  ✅ Set DB_SCHEMA from .env")
                                except Exception as line_error:
                                    log_progress(f"  ⚠️ Error parsing line {line_num}: {line_error}", "warning")
                                    continue
                    
                    env_file_found = True
                    break
                    
                except Exception as e:
                    log_progress(f"⚠️ Error reading .env file {env_path}: {e}", "warning")
                    continue
            else:
                log_progress(f"  ❌ Not found: {env_path}")
        
        if not env_file_found:
            log_progress("⚠️ No .env file found in any location", "warning")
    
    # Final configuration check
    log_progress("🔍 Final configuration check...")
    log_progress(f"  DB_HOST: {'SET' if db_config['host'] else 'MISSING'}")
    log_progress(f"  DB_USER: {'SET' if db_config['user'] else 'MISSING'}")
    log_progress(f"  DB_PASSWORD: {'SET' if db_config['password'] else 'MISSING'}")
    log_progress(f"  DB_PORT: {db_config['port']}")
    log_progress(f"  DB_NAME: {db_config['database']}")
    log_progress(f"  DB_SCHEMA: {db_config['schema']}")
    
    # Validate required fields
    required_fields = ['host', 'user', 'password']
    missing_fields = [field for field in required_fields if not db_config[field]]
    
    if missing_fields:
        log_progress(f"❌ Missing required database configuration: {missing_fields}", "error")
        log_progress("Available environment variables:", "debug")
        for key in ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_SSLMODE', 'DB_SCHEMA']:
            value = os.getenv(key, 'NOT_SET')
            log_progress(f"  {key}: {'***' if 'PASSWORD' in key and value != 'NOT_SET' else value}", "debug")
        
        # Try one more time with direct environment variable injection
        log_progress("🔄 Attempting direct environment variable setup from .env...")
        try:
            env_file_path = '/opt/airflow/.env'
            if os.path.exists(env_file_path):
                with open(env_file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip().strip('"').strip("'")
                            os.environ[key] = value
                
                # Retry config loading
                db_config = {
                    'host': os.getenv('DB_HOST'),
                    'port': os.getenv('DB_PORT', '6543'),
                    'database': os.getenv('DB_NAME', 'postgres'),
                    'user': os.getenv('DB_USER'),
                    'password': os.getenv('DB_PASSWORD'),
                    'sslmode': os.getenv('DB_SSLMODE', 'require'),
                    'schema': os.getenv('DB_SCHEMA', 'google_scraper')
                }
                missing_fields = [field for field in required_fields if not db_config[field]]
                
                if not missing_fields:
                    log_progress("✅ Database config loaded after environment injection")
                else:
                    log_progress(f"❌ Still missing after injection: {missing_fields}", "error")
            
        except Exception as inject_error:
            log_progress(f"⚠️ Environment injection failed: {inject_error}", "warning")
        
        if missing_fields:
            raise ValueError(f"Missing required database configuration: {missing_fields}")
    
    log_progress(f"✅ Database config loaded: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    return db_config

def find_filtered_articles_file(target_date, input_file=None):
    """Find the filtered articles file from the previous task with controlled path support"""
    if input_file and os.path.exists(input_file):
        log_progress(f"✅ Using provided input file: {input_file}")
        return input_file
    
    if isinstance(target_date, str):
        dt = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        dt = target_date
    
    year_str = dt.strftime('%Y')
    month_str = dt.strftime('%m')
    day_str = dt.strftime('%d')
    date_suffix = dt.strftime('%Y-%m-%d')
    date_compact = dt.strftime('%Y%m%d')
    
    # Look for ANY filtered articles file from filtrar_vitimas task (both with and without victims)
    possible_paths = [
        # Primary output from filtrar_vitimas task (articles WITH victims)
        f"/opt/airflow/scripts/google_scraper/data/processed/{year_str}/{month_str}/{day_str}/artigos_vitimas_filtrados_{date_compact}.csv",
        f"/opt/airflow/scripts/google_scraper/data/processed/{year_str}/{month_str}/{day_str}/artigos_vitimas_filtrados_{date_suffix}.csv",
        
        # Secondary output from filtrar_vitimas task (articles WITHOUT victims but still relevant)
        f"/opt/airflow/scripts/google_scraper/data/processed/{year_str}/{month_str}/{day_str}/artigos_sem_vitimas_{date_compact}.csv",
        f"/opt/airflow/scripts/google_scraper/data/processed/{year_str}/{month_str}/{day_str}/artigos_sem_vitimas_{date_suffix}.csv",
        
        # Fallback to processar_relevantes output (all relevant articles)
        f"/opt/airflow/scripts/google_scraper/data/structured/{year_str}/{month_str}/{day_str}/artigos_google_municipios_pt_{date_suffix}.csv",
        
        # Legacy paths
        f"/opt/airflow/scripts/google_scraper/data/processed/artigos_vitimas_filtrados_{date_suffix}.csv",
        f"/opt/airflow/scripts/google_scraper/data/structured/artigos_vitimas_filtrados.csv",
        f"/opt/airflow/scripts/google_scraper/data/structured/artigos_google_municipios_pt.csv"
    ]
    
    found_files = []
    for path in possible_paths:
        if os.path.exists(path):
            file_size = os.path.getsize(path)
            log_progress(f"✅ Found file: {path} ({file_size} bytes)")
            found_files.append((path, file_size))
    
    if not found_files:
        log_progress(f"ℹ️ No filtered articles file found for {date_suffix}. This is normal when no disaster-related events were detected.")
        return None
    
    # Prefer files with victims first, then without victims, then general relevant articles
    # Sort by preference: vitimas_filtrados > sem_vitimas > municipios_pt
    def file_priority(file_path):
        if 'vitimas_filtrados' in file_path:
            return 1
        elif 'sem_vitimas' in file_path:
            return 2
        elif 'municipios_pt' in file_path:
            return 3
        else:
            return 4
    
    # Sort by priority, then by file size (larger files first)
    found_files.sort(key=lambda x: (file_priority(x[0]), -x[1]))
    
    selected_file = found_files[0][0]
    log_progress(f"📋 Selected file for export: {selected_file}")
    
    return selected_file

def create_table_if_not_exists(cursor, schema, table_name, df_columns):
    """Create the table if it doesn't exist with dynamic columns based on input data"""
    # Drop the table first to ensure clean schema (since we're dealing with staging tables)
    drop_table_sql = f"DROP TABLE IF EXISTS {schema}.{table_name};"
    cursor.execute(drop_table_sql)
    log_progress(f"🗑️ Dropped existing table {schema}.{table_name} if it existed")
    
    # Generate column definitions based on input DataFrame
    column_defs = []
    for col in df_columns:
        if col.upper() == 'ID':
            # Use the existing ID column as TEXT primary key (since IDs are hash strings)
            column_defs.append(f"{col} TEXT PRIMARY KEY")
        elif col in ['fatalities', 'injured', 'evacuated', 'displaced', 'missing', 'year', 'month', 'day']:
            column_defs.append(f"{col} INTEGER DEFAULT 0")
        elif col == 'relevance_score':
            column_defs.append(f"{col} REAL DEFAULT 0")
        elif col == 'date':
            column_defs.append(f"{col} DATE")
        elif col in ['DICOFREG']:
            column_defs.append(f"{col} REAL")
        else:
            column_defs.append(f"{col} TEXT")
    
    columns_sql = ',\n        '.join(column_defs)
    
    create_table_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    
    CREATE TABLE {schema}.{table_name} (
        {columns_sql},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    cursor.execute(create_table_sql)
    log_progress(f"✅ Table {schema}.{table_name} created with correct schema")
    
    # Add indexes
    if 'date' in df_columns:
        cursor.execute(f"CREATE INDEX idx_{table_name}_date ON {schema}.{table_name}(date);")
    if 'district' in df_columns:
        cursor.execute(f"CREATE INDEX idx_{table_name}_district ON {schema}.{table_name}(district);")
    if 'evento_nome' in df_columns:
        cursor.execute(f"CREATE INDEX idx_{table_name}_evento ON {schema}.{table_name}(evento_nome);")
    
    log_progress(f"✅ Indexes created for table {schema}.{table_name}")

def convert_date_format(date_str):
    """Convert date from DD/MM/YYYY to YYYY-MM-DD format for PostgreSQL"""
    if pd.isna(date_str) or not date_str:
        return None
    
    try:
        # Handle different possible date formats
        date_str = str(date_str).strip()
        
        # If already in YYYY-MM-DD format, return as is
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        
        # If in DD/MM/YYYY format, convert it
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
            day, month, year = date_str.split('/')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        # If in YYYY/MM/DD format, convert it
        if re.match(r'^\d{4}/\d{1,2}/\d{1,2}$', date_str):
            year, month, day = date_str.split('/')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        # Try parsing with pandas
        parsed_date = pd.to_datetime(date_str, errors='coerce')
        if not pd.isna(parsed_date):
            return parsed_date.strftime('%Y-%m-%d')
        
        log_progress(f"⚠️ Could not parse date: {date_str}", "warning")
        return None
        
    except Exception as e:
        log_progress(f"⚠️ Error converting date '{date_str}': {e}", "warning")
        return None

def prepare_dataframe_for_insert(df):
    """Prepare the dataframe for database insertion with proper data types"""
    log_progress("🔧 Preparing dataframe for database insertion...")
    
    # Create a copy to avoid modifying the original
    df_prepared = df.copy()
    
    # Convert date column to proper format
    if 'date' in df_prepared.columns:
        log_progress("📅 Converting date formats...")
        df_prepared['date'] = df_prepared['date'].apply(convert_date_format)
        # Remove rows with invalid dates
        invalid_dates = df_prepared['date'].isna()
        if invalid_dates.any():
            log_progress(f"⚠️ Removing {invalid_dates.sum()} rows with invalid dates", "warning")
            df_prepared = df_prepared[~invalid_dates]
    
    # Ensure numeric columns are properly converted
    numeric_columns = ['fatalities', 'injured', 'evacuated', 'displaced', 'missing', 'year', 'month', 'day', 'relevance_score']
    for col in numeric_columns:
        if col in df_prepared.columns:
            df_prepared[col] = pd.to_numeric(df_prepared[col], errors='coerce').fillna(0)
            if col in ['fatalities', 'injured', 'evacuated', 'displaced', 'missing', 'year', 'month', 'day']:
                df_prepared[col] = df_prepared[col].astype(int)
    
    # Handle text columns - ensure they're strings and handle NaN
    text_columns = ['title', 'snippet', 'source', 'page', 'district', 'parish', 'evento_nome']
    for col in text_columns:
        if col in df_prepared.columns:
            df_prepared[col] = df_prepared[col].astype(str).replace('nan', '')
            df_prepared[col] = df_prepared[col].replace('None', '')
    
    log_progress(f"✅ Prepared {len(df_prepared)} rows for insertion")
    return df_prepared

def insert_articles(cursor, schema, table_name, articles_df):
    """Insert articles into the database"""
    if articles_df.empty:
        log_progress("⚠️ No articles to insert", "warning")
        return 0
    
    # Prepare the dataframe first
    df_prepared = prepare_dataframe_for_insert(articles_df)
    
    if df_prepared.empty:
        log_progress("⚠️ No valid articles to insert after preparation", "warning")
        return 0
    
    # Use all columns from the input DataFrame
    columns = list(df_prepared.columns)
    
    # Use UPSERT with 'ID' as the unique column if it exists, otherwise use 'page'
    unique_col = 'ID' if 'ID' in columns else ('id' if 'id' in columns else ('page' if 'page' in columns else columns[0]))
    
    # Build the update clause for all columns except the unique one
    update_clauses = []
    for col in columns:
        if col != unique_col:
            update_clauses.append(f"{col} = EXCLUDED.{col}")
    update_clauses.append("updated_at = CURRENT_TIMESTAMP")
    
    insert_sql = f"""
    INSERT INTO {schema}.{table_name} 
    ({', '.join(columns)})
    VALUES ({', '.join(['%s'] * len(columns))})
    ON CONFLICT ({unique_col}) DO UPDATE SET
        {', '.join(update_clauses)}
    """
    
    inserted_count = 0
    error_count = 0
    
    for idx, row in df_prepared.iterrows():
        try:
            values = []
            for col in columns:
                value = row[col]
                # Handle None/NaN values
                if pd.isna(value) or value == 'nan' or value == 'None':
                    if col in ['fatalities', 'injured', 'evacuated', 'displaced', 'missing', 'relevance_score', 'year', 'month', 'day']:
                        values.append(0)
                    else:
                        values.append(None)
                else:
                    values.append(value)
            
            cursor.execute(insert_sql, values)
            inserted_count += 1
            
        except Exception as e:
            error_count += 1
            log_progress(f"⚠️ Error inserting article {idx}: {e}", "warning")
            if error_count <= 3:  # Only show first 3 errors to avoid spam
                log_progress(f"   Row data: {dict(row[columns])}", "debug")
            continue
    
    if error_count > 3:
        log_progress(f"⚠️ ... and {error_count - 3} more errors", "warning")
    
    log_progress(f"✅ Inserted/updated {inserted_count} articles (failed: {error_count})")
    return inserted_count

def save_export_statistics(output_dir, date_str, stats):
    """Save export statistics to controlled output paths"""
    if not output_dir:
        return
    
    try:
        import json
        stats_file = os.path.join(output_dir, f"export_stats_{date_str.replace('-', '')}.json")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Add timestamp and additional metadata
        enhanced_stats = {
            "export_timestamp": datetime.now().isoformat(),
            "export_date": date_str,
            **stats
        }
        
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_stats, f, indent=2, ensure_ascii=False)
        
        log_progress(f"✅ Export statistics saved: {stats_file}")
        
        # Also save a backup CSV for manual inspection
        if 'exported_count' in stats and stats['exported_count'] > 0:
            backup_file = os.path.join(output_dir, f"export_backup_{date_str.replace('-', '')}.csv")
            log_progress(f"📋 Export backup location prepared: {backup_file}")
            
    except Exception as e:
        log_progress(f"⚠️ Could not save export statistics: {e}", "warning")

def export_to_supabase(target_date=None, input_file=None, output_dir=None, date_str=None):
    """Main export function with controlled paths support"""
    if not target_date:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    # Use provided date_str or generate from target_date
    if not date_str:
        date_str = target_date
    
    # Generate table name with compact date format: artigos_filtrados_20250605_staging
    date_compact = date_str.replace('-', '')
    table_name = f"artigos_filtrados_{date_compact}_staging"
    
    log_progress(f"🚀 Starting export to Supabase for date: {target_date}")
    log_progress(f"📋 Target table: {table_name}")
    
    if output_dir:
        log_progress(f"📁 Using controlled output directory: {output_dir}")
    
    # Find the filtered articles file with controlled path support
    input_file_path = find_filtered_articles_file(target_date, input_file)
    if not input_file_path:
        log_progress("ℹ️ No filtered articles file found. This typically means no disaster-related events were detected for this date.")
        log_progress("✅ Export task completed successfully - no data to export.")
        
        # Save statistics indicating no data was available
        if output_dir:
            save_export_statistics(output_dir, date_str, {
                "input_file": None,
                "exported_count": 0,
                "total_input_rows": 0,
                "table_name": table_name,
                "status": "no_data_available",
                "message": "No disaster-related articles found for this date"
            })
        
        return 0  # Return success (0) instead of raising an exception
    
    # Load articles
    try:
        log_progress(f"📂 Loading articles from: {input_file_path}")
        df = pd.read_csv(input_file_path)
        log_progress(f"📊 Loaded {len(df)} articles with columns: {list(df.columns)}")
        
        # Determine file type for logging
        if 'vitimas_filtrados' in input_file_path:
            file_type = "articles with victims"
        elif 'sem_vitimas' in input_file_path:
            file_type = "articles without victims (but disaster-related)"
        elif 'municipios_pt' in input_file_path:
            file_type = "all relevant disaster-related articles"
        else:
            file_type = "filtered articles"
        
        log_progress(f"📋 Processing {file_type}")
        
        if df.empty:
            log_progress("ℹ️ Articles file is empty - no disaster-related events detected.")
            log_progress("✅ Export task completed successfully - no data to export.")
            # Save empty export statistics
            if output_dir:
                save_export_statistics(output_dir, date_str, {
                    "input_file": input_file_path,
                    "exported_count": 0,
                    "total_input_rows": 0,
                    "table_name": table_name,
                    "file_type": file_type,
                    "status": "no_data_empty_file",
                    "message": "Input file was empty - no disaster-related articles found"
                })
            return 0
        
    except Exception as e:
        log_progress(f"❌ Error loading articles file: {e}", "error")
        raise
    
    # Get database configuration
    try:
        db_config = get_database_config()
    except Exception as e:
        log_progress(f"❌ Database configuration error: {e}", "error")
        raise
    
    # Connect to database and export
    connection = None
    try:
        log_progress(f"🔗 Connecting to database...")
        connection = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            sslmode=db_config['sslmode']
        )
        connection.autocommit = True
        cursor = connection.cursor()
        
        log_progress(f"✅ Connected to database successfully")
        
        # Create table if not exists with dynamic columns
        create_table_if_not_exists(cursor, db_config['schema'], table_name, df.columns)
        
        # Insert articles
        inserted_count = insert_articles(cursor, db_config['schema'], table_name, df)
        
        # Create backup CSV in output directory
        backup_file = None
        if output_dir:
            try:
                os.makedirs(output_dir, exist_ok=True)
                backup_file = os.path.join(output_dir, f"export_backup_{date_str.replace('-', '')}.csv")
                df.to_csv(backup_file, index=False)
                log_progress(f"💾 Export backup saved: {backup_file}")
            except Exception as e:
                log_progress(f"⚠️ Could not save backup CSV: {e}", "warning")
        
        # Save export statistics to controlled paths
        export_stats = {
            "input_file": input_file_path,
            "exported_count": inserted_count,
            "total_input_rows": len(df),
            "table_name": f"{db_config['schema']}.{table_name}",
            "database_host": db_config['host'],
            "file_type": file_type,
            "backup_file": backup_file,
            "columns": list(df.columns),
            "status": "success"
        }
        
        if output_dir:
            save_export_statistics(output_dir, date_str, export_stats)
        
        log_progress(f"✅ Export completed successfully. Inserted/updated {inserted_count} {file_type} into table {table_name}")
        return inserted_count
        
    except Exception as e:
        log_progress(f"❌ Database operation failed: {e}", "error")
        
        # Save error statistics
        if output_dir:
            save_export_statistics(output_dir, date_str, {
                "input_file": input_file_path if 'input_file_path' in locals() else input_file,
                "exported_count": 0,
                "table_name": table_name,
                "error": str(e),
                "status": "failed"
            })
        
        raise
    finally:
        if connection:
            connection.close()
            log_progress("🔗 Database connection closed")

def main():
    """Main function with controlled output paths support"""
    parser = argparse.ArgumentParser(description="Export filtered articles to Supabase (Airflow version)")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--input_file", type=str, help="Specific input file path")
    parser.add_argument("--output_dir", type=str, help="Output directory for export logs and statistics")
    parser.add_argument("--date_str", type=str, help="Date string for file naming")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        log_progress("🔍 DEBUG logging enabled", "debug")
    
    log_progress("Starting export_to_supabase_airflow")
    log_progress(f"Parameters: date={args.date}")
    log_progress(f"Paths: input_file={args.input_file}, output_dir={args.output_dir}, date_str={args.date_str}")
    
    try:
        result = export_to_supabase(
            target_date=args.date,
            input_file=args.input_file,
            output_dir=args.output_dir,
            date_str=args.date_str
        )
        log_progress(f"✅ Export completed successfully. Processed {result} articles")
        return 0
    except Exception as e:
        log_progress(f"❌ Export failed: {e}", "error")
        import traceback
        log_progress(f"❌ Full traceback: {traceback.format_exc()}", "error")
        return 1

if __name__ == "__main__":
    sys.exit(main())
