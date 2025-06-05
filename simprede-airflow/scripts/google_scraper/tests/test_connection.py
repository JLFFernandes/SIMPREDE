import os
import psycopg2
import time
from dotenv import load_dotenv

# 🔐 Carregar variáveis do .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 6543))
DB_NAME = os.getenv("DB_NAME")
DB_SCHEMA = os.getenv("DB_SCHEMA", "google_scraper")

print("🔧 Testing Supabase connection...")

# Method 1: Using connection parameters
try:
    print("🔌 Method 1: Using connection parameters...")
    start_time = time.time()
    
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        sslmode="require",
        connect_timeout=10
    )
    
    end_time = time.time()
    print(f"✅ Connection successful using parameters! Time: {end_time - start_time:.2f}s")
    
    # Test a simple query
    cur = conn.cursor()
    cur.execute("SELECT current_database(), current_schema()")
    result = cur.fetchone()
    print(f"📊 Current database: {result[0]}, Current schema: {result[1]}")
    
    # Check if schema exists
    cur.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{DB_SCHEMA}'")
    schema_exists = cur.fetchone()
    if schema_exists:
        print(f"✅ Schema '{DB_SCHEMA}' exists")
    else:
        print(f"⚠️ Schema '{DB_SCHEMA}' does not exist!")
    
    conn.close()
    print("🔌 Connection closed")
    
except Exception as e:
    print(f"❌ Connection failed using parameters: {e}")

# Method 2: Using connection string
try:
    print("\n🔌 Method 2: Using connection string...")
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    start_time = time.time()
    
    conn = psycopg2.connect(
        connection_string,
        sslmode="require",
        connect_timeout=10
    )
    
    end_time = time.time()
    print(f"✅ Connection successful using connection string! Time: {end_time - start_time:.2f}s")
    
    # Test a simple query
    cur = conn.cursor()
    cur.execute("SELECT current_database(), current_schema()")
    result = cur.fetchone()
    print(f"📊 Current database: {result[0]}, Current schema: {result[1]}")
    
    conn.close()
    print("🔌 Connection closed")
    
except Exception as e:
    print(f"❌ Connection failed using connection string: {e}")
