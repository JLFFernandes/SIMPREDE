import sys
import os
import logging
import argparse
import pandas as pd
import psycopg2
import re
from datetime import datetime, timedelta
from pathlib import Path

# Set unbuffered output for Airflow compatibility
os.environ['PYTHONUNBUFFERED'] = '1'
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(line_buffering=True)

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
        
        env_file_found = False
        for env_path in possible_env_paths:
            if os.path.exists(env_path):
                log_progress(f"✅ Found .env file at: {env_path}")
                try:
                    # Simple .env parser
                    with open(env_path, 'r') as f:
                        for line in f:
                            line = line.strip()
                            if line and not line.startswith('#') and '=' in line:
                                key, value = line.split('=', 1)
                                key = key.strip()
                                value = value.strip()
                                if key == 'DB_HOST' and not db_config['host']:
                                    db_config['host'] = value
                                elif key == 'DB_PORT':
                                    db_config['port'] = value
                                elif key == 'DB_NAME':
                                    db_config['database'] = value
                                elif key == 'DB_USER' and not db_config['user']:
                                    db_config['user'] = value
                                elif key == 'DB_PASSWORD' and not db_config['password']:
                                    db_config['password'] = value
                                elif key == 'DB_SSLMODE':
                                    db_config['sslmode'] = value
                                elif key == 'DB_SCHEMA':
                                    db_config['schema'] = value
                    env_file_found = True
                    break
                except Exception as e:
                    log_progress(f"⚠️ Error reading .env file {env_path}: {e}", "warning")
                    continue
        
        if not env_file_found:
            log_progress("⚠️ No .env file found in any location", "warning")
    
    # Validate required fields
    required_fields = ['host', 'user', 'password']
    missing_fields = [field for field in required_fields if not db_config[field]]
    
    if missing_fields:
        log_progress(f"❌ Missing required database configuration: {missing_fields}", "error")
        log_progress("Available environment variables:", "debug")
        for key in ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_SSLMODE', 'DB_SCHEMA']:
            value = os.getenv(key, 'NOT_SET')
            log_progress(f"  {key}: {'***' if 'PASSWORD' in key and value != 'NOT_SET' else value}", "debug")
        raise ValueError(f"Missing required database configuration: {missing_fields}")
    
    log_progress(f"✅ Database config loaded: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    return db_config

def find_filtered_articles_file(target_date):
    """Find the filtered articles file from the previous task"""
    if isinstance(target_date, str):
        dt = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        dt = target_date
    
    year_str = dt.strftime('%Y')
    month_str = dt.strftime('%m')
    day_str = dt.strftime('%d')
    date_suffix = dt.strftime('%Y-%m-%d')
    
    # Possible file locations in Airflow container
    possible_paths = [
        # Airflow data directories (preferred)
        f"/opt/airflow/data/structured/{year_str}/{month_str}/{day_str}/artigos_vitimas_filtrados_{date_suffix}.csv",
        f"/opt/airflow/data/structured/artigos_vitimas_filtrados.csv",
        
        # Script directories
        f"/opt/airflow/scripts/google_scraper/data/structured/{year_str}/{month_str}/{day_str}/artigos_vitimas_filtrados_{date_suffix}.csv",
        f"/opt/airflow/scripts/google_scraper/data/structured/artigos_vitimas_filtrados.csv",
        
        # Legacy paths with different date format
        f"/opt/airflow/scripts/google_scraper/data/structured/{year_str}/{month_str}/{day_str}/artigos_vitimas_filtrados_{dt.strftime('%Y%m%d')}.csv",
        
        # Any recent files in structured directories
        f"/opt/airflow/scripts/google_scraper/data/structured/artigos_vitimas_filtrados_{date_suffix}.csv"
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            log_progress(f"✅ Found filtered articles file: {path}")
            return path
    
    log_progress(f"❌ No filtered articles file found. Searched:", "error")
    for path in possible_paths:
        log_progress(f"   - {path}", "error")
    
    # Try to find any file with similar pattern
    search_dirs = [
        f"/opt/airflow/data/structured/{year_str}/{month_str}/{day_str}",
        "/opt/airflow/data/structured",
        f"/opt/airflow/scripts/google_scraper/data/structured/{year_str}/{month_str}/{day_str}",
        "/opt/airflow/scripts/google_scraper/data/structured"
    ]
    
    for search_dir in search_dirs:
        if os.path.exists(search_dir):
            files = [f for f in os.listdir(search_dir) if 'artigos_vitimas_filtrados' in f and f.endswith('.csv')]
            if files:
                found_file = os.path.join(search_dir, files[0])
                log_progress(f"🔄 Found alternative file: {found_file}")
                return found_file
    
    return None

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

def export_to_supabase(target_date=None):
    """Main export function"""
    if not target_date:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    # Generate table name with date (remove dashes for compact format)
    date_compact = target_date.replace('-', '')
    table_name = f"artigos_filtrados_{date_compact}_staging"
    
    log_progress(f"🚀 Starting export to Supabase for date: {target_date}")
    log_progress(f"📋 Target table: {table_name}")
    
    # Find the filtered articles file
    input_file = find_filtered_articles_file(target_date)
    if not input_file:
        log_progress("❌ No filtered articles file found. Cannot proceed.", "error")
        return 0
    
    # Load articles
    try:
        log_progress(f"📂 Loading articles from: {input_file}")
        df = pd.read_csv(input_file)
        log_progress(f"📊 Loaded {len(df)} articles with columns: {list(df.columns)}")
        
        if df.empty:
            log_progress("⚠️ No articles to export", "warning")
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
        
        log_progress(f"✅ Export completed successfully. Inserted/updated {inserted_count} articles into {table_name}")
        return inserted_count
        
    except Exception as e:
        log_progress(f"❌ Database operation failed: {e}", "error")
        raise
    finally:
        if connection:
            connection.close()
            log_progress("🔗 Database connection closed")

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Export filtered articles to Supabase (Airflow version)")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        log_progress("🔍 DEBUG logging enabled", "debug")
    
    log_progress("Starting export_to_supabase_airflow")
    log_progress(f"Parameters: date={args.date}")
    
    try:
        result = export_to_supabase(target_date=args.date)
        log_progress(f"✅ Export completed successfully. Processed {result} articles")
        return 0
    except Exception as e:
        log_progress(f"❌ Export failed: {e}", "error")
        import traceback
        log_progress(f"❌ Full traceback: {traceback.format_exc()}", "error")
        return 1

if __name__ == "__main__":
    sys.exit(main())
