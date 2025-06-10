import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
from typing import List, Dict, Any, Literal
from dotenv import load_dotenv
import geopandas as gpd

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CentroidsExporter:
    def __init__(self):
        # Load database credentials from environment
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT', '6543')
        self.db_name = os.getenv('DB_NAME')
        
        if not all([self.db_user, self.db_password, self.db_host, self.db_name]):
            raise ValueError("Database credentials must be set in environment variables")
        
        # Create connection string
        self.connection_string = f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        self.engine = create_engine(self.connection_string)
    
    def read_centroids_from_csv(self, file_path: str) -> pd.DataFrame:
        """Read centroids from CSV file"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Read {len(df)} centroids from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
    
    def read_centroids_from_query(self, query: str) -> pd.DataFrame:
        """Read centroids from database using custom query"""
        try:
            df = pd.read_sql(query, self.engine)
            logger.info(f"Read {len(df)} centroids from database")
            return df
        except Exception as e:
            logger.error(f"Error reading from database: {e}")
            raise
    
    def read_centroids_from_shapefile(self, shapefile_path: str) -> pd.DataFrame:
        """Read centroids from shapefile"""
        try:
            logger.info(f"Attempting to read shapefile: {shapefile_path}")
            
            # Check if file exists
            if not os.path.exists(shapefile_path):
                raise FileNotFoundError(f"Shapefile not found: {shapefile_path}")
            
            # Read shapefile using geopandas
            gdf = gpd.read_file(shapefile_path)
            logger.info(f"Successfully read shapefile with {len(gdf)} features")
            logger.info(f"Original CRS: {gdf.crs}")
            
            # Check if GeoDataFrame is empty
            if len(gdf) == 0:
                logger.warning("Shapefile contains no features!")
                return pd.DataFrame()
            
            # Convert to WGS84 (EPSG:4326) if not already in that CRS
            if gdf.crs != 'EPSG:4326':
                logger.info(f"Converting from {gdf.crs} to EPSG:4326")
                gdf = gdf.to_crs('EPSG:4326')
            
            # Extract coordinates from geometry
            gdf['longitude'] = gdf.geometry.x
            gdf['latitude'] = gdf.geometry.y
            
            logger.info(f"Extracted coordinates - Lat range: {gdf['latitude'].min():.4f} to {gdf['latitude'].max():.4f}")
            logger.info(f"Extracted coordinates - Lon range: {gdf['longitude'].min():.4f} to {gdf['longitude'].max():.4f}")
            
            # Convert to regular DataFrame and select relevant columns
            df = pd.DataFrame(gdf.drop(columns=['geometry']))
            
            logger.info(f"Read {len(df)} centroids from {shapefile_path}")
            logger.info(f"Columns available: {list(df.columns)}")
            
            return df
        except Exception as e:
            logger.error(f"Error reading shapefile: {e}")
            raise

    def export_to_supabase(self, centroids_df: pd.DataFrame, table_name: str = 'centroids', 
                          if_exists: Literal['fail', 'replace', 'append'] = 'replace') -> bool:
        """Export centroids to Supabase public table"""
        try:
            logger.info(f"Attempting to export {len(centroids_df)} rows to {table_name}")
            logger.info(f"DataFrame columns: {list(centroids_df.columns)}")
            logger.info(f"DataFrame shape: {centroids_df.shape}")
            logger.info(f"First few rows:\n{centroids_df.head()}")

            # Check for null values in required columns
            null_lat = centroids_df['latitude'].isnull().sum()
            null_lon = centroids_df['longitude'].isnull().sum()
            if null_lat > 0 or null_lon > 0:
                logger.warning(f"Found {null_lat} null latitudes and {null_lon} null longitudes")
                # Remove rows with null coordinates
                centroids_df = centroids_df.dropna(subset=['latitude', 'longitude'])
                logger.info(f"After removing null coordinates: {len(centroids_df)} rows remaining")

            # Use sqlalchemy Table for insertion instead of to_sql
            from sqlalchemy import MetaData, Table
            metadata = MetaData(bind=self.engine)
            metadata.reflect()
            centroids_table = metadata.tables.get(table_name)

            if centroids_table is None:
                raise ValueError(f"Table '{table_name}' not found in database.")

            # If replacing, clear table before inserting
            if if_exists == 'replace':
                with self.engine.begin() as conn:
                    conn.execute(f"TRUNCATE TABLE public.{table_name} RESTART IDENTITY CASCADE;")
                    logger.info(f"Table public.{table_name} truncated before insert (replace mode)")

            # Prepare data
            rows = centroids_df.to_dict(orient="records")

            # Insert rows
            with self.engine.begin() as conn:
                if rows:
                    conn.execute(centroids_table.insert(), rows)
                    logger.info(f"Inserted {len(rows)} rows into public.{table_name}")
                else:
                    logger.warning("No rows to insert after filtering null coordinates.")

            # Verify the data was inserted
            from sqlalchemy import text
            verification_query = text(f"SELECT COUNT(*) as count FROM public.{table_name}")
            with self.engine.connect() as conn:
                result = conn.execute(verification_query)
                row_count = result.scalar()
                logger.info(f"Verification: {row_count} rows found in public.{table_name}")

            return True

        except Exception as e:
            logger.error(f"Error exporting to Supabase: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    def create_centroids_table_if_not_exists(self):
        """Create centroids table with proper schema if it doesn't exist"""
        
        # Check if table exists and get its structure
        check_table_sql = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'centroids'
        ORDER BY ordinal_position;
        """
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(check_table_sql)
                existing_columns = [row[0] for row in result] if result else []
                
                if existing_columns:
                    logger.info(f"Table exists with columns: {existing_columns}")
                    # If table exists but missing required columns, recreate it
                    required_columns = ['distrito', 'concelho', 'freguesia', 'centroid_type']
                    missing_columns = [col for col in required_columns if col not in existing_columns]
                    
                    if missing_columns:
                        logger.info(f"Missing columns: {missing_columns}. Recreating table...")
                        # Drop and recreate table
                        conn.execute("DROP TABLE IF EXISTS public.centroids CASCADE;")
                        logger.info("Dropped existing table")
                else:
                    logger.info("Table does not exist. Creating new table...")
                
                # Create the table (whether it's new or recreated)
                from sqlalchemy import text
                create_table_sql = text("""
                CREATE TABLE IF NOT EXISTS public.centroids (
                    id SERIAL PRIMARY KEY,
                    latitude DOUBLE PRECISION NOT NULL,
                    longitude DOUBLE PRECISION NOT NULL,
                    name VARCHAR(255),
                    region VARCHAR(255),
                    distrito VARCHAR(255),
                    concelho VARCHAR(255),
                    freguesia VARCHAR(255),
                    dicofre VARCHAR(50),
                    area_ha DOUBLE PRECISION,
                    cluster_id INTEGER,
                    centroid_type VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                
                conn.execute(create_table_sql)
                logger.info("Centroids table created successfully")
                
                # Create indexes
                indexes_sql = text("""
                CREATE INDEX IF NOT EXISTS idx_centroids_coords ON public.centroids (latitude, longitude);
                CREATE INDEX IF NOT EXISTS idx_centroids_region ON public.centroids (distrito, concelho);
                CREATE INDEX IF NOT EXISTS idx_centroids_type ON public.centroids (centroid_type);
                """)
                
                conn.execute(indexes_sql)
                logger.info("Indexes created successfully")
                
        except Exception as e:
            logger.error(f"Error creating centroids table: {e}")
            raise
    
    def process_shapefile_data(self, df: pd.DataFrame, centroid_type: str) -> pd.DataFrame:
        """Process and standardize shapefile data"""
        logger.info(f"Processing {centroid_type} shapefile data with {len(df)} rows")
        logger.info(f"Original columns: {list(df.columns)}")
        
        processed_df = df.copy()
        
        # Add centroid type
        processed_df['centroid_type'] = centroid_type
        
        # Standardize column names based on the shapefile type
        if centroid_type == 'freguesia':
            # Map freguesia shapefile columns
            column_mapping = {
                'Freguesia': 'freguesia',
                'Concelho': 'concelho', 
                'Distrito': 'distrito',
                'Dicofre': 'dicofre',
                'AREA_T_Ha': 'area_ha'
            }
        elif centroid_type == 'concelho':
            # Map concelho shapefile columns
            column_mapping = {
                'NAME_2': 'concelho',
                'NAME_1': 'distrito',
                'CCA_2': 'dicofre'
            }
        else:
            column_mapping = {}
        
        # Rename columns
        for old_col, new_col in column_mapping.items():
            if old_col in processed_df.columns:
                processed_df[new_col] = processed_df[old_col]
                logger.info(f"Mapped {old_col} -> {new_col}")
        
        # Create a name field
        if 'freguesia' in processed_df.columns:
            processed_df['name'] = processed_df['freguesia']
            processed_df['region'] = processed_df['distrito']
        elif 'concelho' in processed_df.columns:
            processed_df['name'] = processed_df['concelho']
            processed_df['region'] = processed_df['distrito']
        else:
            processed_df['name'] = f"{centroid_type}_centroid"
            processed_df['region'] = 'Portugal'
        
        # Ensure we have latitude and longitude
        if 'latitude' not in processed_df.columns or 'longitude' not in processed_df.columns:
            logger.error("Missing latitude or longitude columns!")
            raise ValueError("Latitude and longitude columns are required")
        
        # Select only the columns we want to keep
        columns_to_keep = [
            'latitude', 'longitude', 'name', 'region', 
            'distrito', 'concelho', 'freguesia', 'dicofre', 
            'area_ha', 'centroid_type'
        ]
        
        # Keep only existing columns
        final_columns = [col for col in columns_to_keep if col in processed_df.columns]
        processed_df = processed_df[final_columns]
        
        logger.info(f"Processed {len(processed_df)} {centroid_type} centroids")
        logger.info(f"Final columns: {list(processed_df.columns)}")
        logger.info(f"Sample data:\n{processed_df.head()}")
        
        return processed_df

    def run_shapefile_export(self, shapefile_path: str, centroid_type: str, replace_existing: bool = True):
        """Main method to run shapefile to Supabase export"""
        try:
            # Create table if needed
            self.create_centroids_table_if_not_exists()
            
            # Read shapefile data
            centroids_df = self.read_centroids_from_shapefile(shapefile_path)
            
            # Process the data
            processed_df = self.process_shapefile_data(centroids_df, centroid_type)
            
            # Export data
            if_exists = 'replace' if replace_existing else 'append'
            success = self.export_to_supabase(processed_df, if_exists=if_exists)
            
            if success:
                logger.info(f"{centroid_type.capitalize()} shapefile export completed successfully")
            else:
                logger.error(f"{centroid_type.capitalize()} shapefile export failed")
                
        except Exception as e:
            logger.error(f"{centroid_type.capitalize()} shapefile export process failed: {e}")

def create_sample_data_and_export(exporter):
    """Create sample centroids data and export to test the functionality"""
    import numpy as np
    
    # Generate sample centroids data for Portugal
    np.random.seed(42)  # For reproducible results
    
    # Portugal bounding box (approximate)
    lat_min, lat_max = 36.8, 42.2
    lon_min, lon_max = -9.5, -6.2
    
    n_centroids = 20
    sample_data = {
        'latitude': np.random.uniform(lat_min, lat_max, n_centroids),
        'longitude': np.random.uniform(lon_min, lon_max, n_centroids),
        'name': [f'Centroid_{i+1}' for i in range(n_centroids)],
        'region': np.random.choice(['Norte', 'Centro', 'Lisboa', 'Alentejo', 'Algarve'], n_centroids),
        'cluster_id': np.random.randint(1, 6, n_centroids)
    }
    
    df = pd.DataFrame(sample_data)
    
    print("Generated sample centroids data:")
    print(df.head())
    print(f"\nTotal centroids: {len(df)}")
    
    # Export to Supabase
    success = exporter.export_to_supabase(df, if_exists='replace')
    
    if success:
        print("\n✅ Sample centroids exported successfully to Supabase!")
        print("You can now view them in your Supabase dashboard at:")
        print("https://kyrfsylobmsdjlrrpful.supabase.co/project/default/editor")
    else:
        print("\n❌ Failed to export sample centroids")

def main():
    """Example usage"""
    exporter = CentroidsExporter()
    
    # First, let's try with sample data to ensure the export process works
    print("Testing with sample data first...")
    create_sample_data_and_export(exporter)
    
    # Option 1: Export freguesia centroids
    freguesia_shapefile = "/Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow/scripts/centroides/centroide_freg/shape_freg_centroide.shp"
    if os.path.exists(freguesia_shapefile):
        print("\nExporting freguesia centroids...")
        exporter.run_shapefile_export(freguesia_shapefile, 'freguesia', replace_existing=False)  # Append to sample data
    else:
        print(f"Freguesia shapefile not found at: {freguesia_shapefile}")
    
    # Option 2: Export concelho centroids  
    concelho_shapefile = "/Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow/scripts/centroides/centroide_concel/shape_conc_centroide.shp"
    if os.path.exists(concelho_shapefile):
        print("\nExporting concelho centroids...")
        exporter.run_shapefile_export(concelho_shapefile, 'concelho', replace_existing=False)  # Append to existing data
    else:
        print(f"Concelho shapefile not found at: {concelho_shapefile}")

if __name__ == "__main__":
    main()
