from utils.supabase_connector import SupabaseConnection

def test_supabase_connection():
    try:
        conn = SupabaseConnection()
        print("✅ Supabase connection initialized successfully")
        
        # Test basic connection
        conn.test_connection()
        
        # Try to fetch some data
        df = conn.get_business_data()
        print(f"📊 Fetched {len(df)} rows of business data")
        if not df.empty:
            print(f"📋 Columns available: {list(df.columns)}")
            print(f"🔍 First few rows:")
            print(df.head())
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    test_supabase_connection()