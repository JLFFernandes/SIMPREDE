import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables from the project root
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(dotenv_path=env_path)

class SupabaseConnection:
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_ANON_KEY')
        
        if not self.url or not self.key:
            raise ValueError("Supabase credentials not found in environment variables")
            
        self.client: Client = create_client(self.url, self.key)
    
    def get_ocorrencias_data(self):
        """Fetch emergency events data excluding 'Other' type"""
        try:
            response = self.client.table('google_scraper_ocorrencias').select('*').neq('type', 'Other').execute()
            return pd.DataFrame(response.data)
        except Exception as e:
            return pd.DataFrame()
    
    def get_location_data(self):
        """Fetch location data for mapping excluding 'Other' type"""
        try:
            response = self.client.table('google_scraper_ocorrencias').select(
                'id, type, subtype, latitude, longitude, district, municipality, parish, fatalities, injured, date'
            ).neq('type', 'Other').execute()
            
            df = pd.DataFrame(response.data)
            
            if not df.empty:
                df = df.dropna(subset=['latitude', 'longitude'])
                df = df[
                    (df['latitude'].between(-90, 90)) & 
                    (df['longitude'].between(-180, 180))
                ]
            
            return df
        except Exception as e:
            return pd.DataFrame()
    
    def get_eswd_data(self):
        """Fetch ALL ESWD data by joining tables manually - using pagination to overcome Supabase 1000 row limit"""
        try:
            # Function to fetch all records from a table using pagination
            def fetch_all_records(table_name):
                all_records = []
                page_size = 1000
                offset = 0
                
                while True:
                    try:
                        response = self.client.table(table_name).select('*').range(offset, offset + page_size - 1).execute()
                        batch = response.data
                        
                        if not batch:
                            break
                            
                        all_records.extend(batch)
                        
                        if len(batch) < page_size:
                            break
                            
                        offset += page_size
                        
                    except Exception as e:
                        break
                
                return all_records
            
            # Fetch ALL disasters data with pagination
            disasters_data = fetch_all_records('disasters')
            disasters_df = pd.DataFrame(disasters_data)
            
            if disasters_df.empty:
                return pd.DataFrame()
            
            # Fetch ALL location data
            try:
                locations_data = fetch_all_records('location')
                location_df = pd.DataFrame(locations_data)
                
                # Merge disasters with location data
                if not location_df.empty:
                    merged_df = pd.merge(disasters_df, location_df, on='id', how='left', suffixes=('', '_loc'))
                else:
                    merged_df = disasters_df
                    
            except Exception as e:
                merged_df = disasters_df
            
            # Fetch ALL human impacts data
            try:
                impacts_data = fetch_all_records('human_impacts')
                impacts_df = pd.DataFrame(impacts_data)
                
                if not impacts_df.empty:
                    merged_df = pd.merge(merged_df, impacts_df, on='id', how='left', suffixes=('', '_hi'))
                    
            except Exception as e:
                pass
            
            # Fetch ALL information sources data
            try:
                sources_data = fetch_all_records('information_sources')
                sources_df = pd.DataFrame(sources_data)
                
                if not sources_df.empty:
                    # Join on disaster_id instead of id for sources
                    sources_df = sources_df.rename(columns={'disaster_id': 'id'})
                    merged_df = pd.merge(merged_df, sources_df, on='id', how='left', suffixes=('', '_src'))
                    
            except Exception as e:
                pass
            
            return merged_df
            
        except Exception as e:
            return pd.DataFrame()
    
    def get_eswd_data_paginated(self):
        """Alternative method: Fetch ALL ESWD data using pagination to get all records"""
        try:
            # Fetch all disasters using pagination
            all_disasters = []
            page_size = 1000
            offset = 0
            
            while True:
                response = self.client.table('disasters').select('*').range(offset, offset + page_size - 1).execute()
                batch = response.data
                
                if not batch:
                    break
                    
                all_disasters.extend(batch)
                
                if len(batch) < page_size:
                    break
                    
                offset += page_size
            
            disasters_df = pd.DataFrame(all_disasters)
            
            if disasters_df.empty:
                return pd.DataFrame()
            
            # Fetch all location data using pagination
            try:
                all_locations = []
                offset = 0
                
                while True:
                    response = self.client.table('location').select('*').range(offset, offset + page_size - 1).execute()
                    batch = response.data
                    
                    if not batch:
                        break
                        
                    all_locations.extend(batch)
                    
                    if len(batch) < page_size:
                        break
                        
                    offset += page_size
                
                location_df = pd.DataFrame(all_locations)
                
                # Merge with locations
                if not location_df.empty:
                    merged_df = pd.merge(disasters_df, location_df, on='id', how='left', suffixes=('', '_loc'))
                else:
                    merged_df = disasters_df
                    
            except Exception as e:
                merged_df = disasters_df
            
            # Fetch all human impacts using pagination
            try:
                all_impacts = []
                offset = 0
                
                while True:
                    response = self.client.table('human_impacts').select('*').range(offset, offset + page_size - 1).execute()
                    batch = response.data
                    
                    if not batch:
                        break
                        
                    all_impacts.extend(batch)
                    
                    if len(batch) < page_size:
                        break
                        
                    offset += page_size
                
                impacts_df = pd.DataFrame(all_impacts)
                
                if not impacts_df.empty:
                    merged_df = pd.merge(merged_df, impacts_df, on='id', how='left', suffixes=('', '_hi'))
                    
            except Exception as e:
                pass
            
            # Fetch all information sources using pagination
            try:
                all_sources = []
                offset = 0
                
                while True:
                    response = self.client.table('information_sources').select('*').range(offset, offset + page_size - 1).execute()
                    batch = response.data
                    
                    if not batch:
                        break
                        
                    all_sources.extend(batch)
                    
                    if len(batch) < page_size:
                        break
                        
                    offset += page_size
                
                sources_df = pd.DataFrame(all_sources)
                
                if not sources_df.empty:
                    # Join on disaster_id instead of id for sources
                    sources_df = sources_df.rename(columns={'disaster_id': 'id'})
                    merged_df = pd.merge(merged_df, sources_df, on='id', how='left', suffixes=('', '_src'))
                    
            except Exception as e:
                pass
            
            return merged_df
            
        except Exception as e:
            return pd.DataFrame()
    
    def get_summary_stats(self):
        """Get summary statistics excluding 'Other' type"""
        try:
            response = self.client.table('google_scraper_ocorrencias').select('*').neq('type', 'Other').execute()
            df = pd.DataFrame(response.data)
            
            if df.empty:
                return {}
            
            stats = {
                'total_events': len(df),
                'total_fatalities': df['fatalities'].sum(),
                'total_injured': df['injured'].sum(),
                'event_types': df['type'].value_counts().to_dict(),
                'districts': df['district'].value_counts().to_dict(),
                'recent_events': df.nlargest(5, 'created_at').to_dict('records')
            }
            
            return stats
        except Exception as e:
            return {}
    
    def get_eswd_table_counts(self):
        """Get the total count of records in each ESWD table for debugging"""
        try:
            counts = {}
            
            # Count disasters
            try:
                disasters_count = self.client.table('disasters').select('id', count='exact').execute()
                counts['disasters'] = disasters_count.count
            except Exception as e:
                counts['disasters'] = 0
            
            # Count locations
            try:
                locations_count = self.client.table('location').select('id', count='exact').execute()
                counts['location'] = locations_count.count
            except Exception as e:
                counts['location'] = 0
            
            # Count human impacts
            try:
                impacts_count = self.client.table('human_impacts').select('id', count='exact').execute()
                counts['human_impacts'] = impacts_count.count
            except Exception as e:
                counts['human_impacts'] = 0
            
            # Count information sources
            try:
                sources_count = self.client.table('information_sources').select('disaster_id', count='exact').execute()
                counts['information_sources'] = sources_count.count
            except Exception as e:
                counts['information_sources'] = 0
            
            return counts
            
        except Exception as e:
            return {}
