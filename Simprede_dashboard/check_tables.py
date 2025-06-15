from utils.supabase_connector import SupabaseConnection
import pandas as pd

def check_google_scraper_tables():
    try:
        conn = SupabaseConnection()
        print("✅ Checking Google Scraper tables...")
        
        # Check the main google_scraper_ocorrencias table
        try:
            response = conn.client.table('google_scraper_ocorrencias').select('*').limit(5).execute()
            if response.data:
                print("✅ Found google_scraper_ocorrencias table")
                df = pd.DataFrame(response.data)
                print(f"   Columns: {list(df.columns)}")
                print(f"   Total rows: {len(response.data)}")
                print(f"   Sample data:")
                for i, row in enumerate(response.data[:2]):
                    print(f"   Row {i+1}: {row}")
                return df
            else:
                print("❌ google_scraper_ocorrencias table is empty")
        except Exception as e:
            print(f"❌ Error accessing google_scraper_ocorrencias: {e}")
        
        # Check for other related tables
        related_tables = [
            'google_scraper',
            'ocorrencias', 
            'scraped_businesses',
            'google_places_data',
            'business_locations'
        ]
        
        print("\n🔍 Checking related tables...")
        for table in related_tables:
            try:
                response = conn.client.table(table).select('*').limit(1).execute()
                if response.data:
                    print(f"✅ Found table: {table}")
                    df = pd.DataFrame(response.data)
                    print(f"   Columns: {list(df.columns)}")
            except Exception as e:
                if "does not exist" in str(e):
                    print(f"❌ Table {table} does not exist")
                else:
                    print(f"❌ Error accessing {table}: {e}")
        
        return None
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def check_combined_query_tables():
    """Check for tables that might be outputs from SQL queries"""
    try:
        conn = SupabaseConnection()
        print("\n🔍 Checking for combined/processed tables...")
        
        # These might be outputs from your SQL queries DAG
        query_output_tables = [
            'disaster_events',
            'processed_events', 
            'combined_disasters',
            'analytics_summary',
            'dashboard_data',
            'eventos_processados',
            'dados_consolidados'
        ]
        
        for table in query_output_tables:
            try:
                response = conn.client.table(table).select('*').limit(1).execute()
                if response.data:
                    print(f"✅ Found processed table: {table}")
                    df = pd.DataFrame(response.data)
                    print(f"   Columns: {list(df.columns)}")
            except Exception as e:
                if "does not exist" not in str(e):
                    print(f"❌ Error accessing {table}: {e}")
                    
    except Exception as e:
        print(f"❌ Error checking combined tables: {e}")

if __name__ == "__main__":
    # Check the main google scraper table
    df = check_google_scraper_tables()
    
    # Check for combined/processed tables
    check_combined_query_tables()
    
    # If we found data, show some analysis
    if df is not None and not df.empty:
        print(f"\n📊 Analysis of google_scraper_ocorrencias:")
        print(f"   Shape: {df.shape}")
        print(f"   Columns with location data:")
        for col in df.columns:
            if any(term in col.lower() for term in ['lat', 'lng', 'longitude', 'latitude', 'coord', 'geo']):
                print(f"     - {col}")