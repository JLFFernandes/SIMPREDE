#!/usr/bin/env python3
# filepath: /Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow/dags/google_scraper/exportador_bd/export_to_supabase_airflow.py
"""
Versão compatível com Airflow do export_to_supabase.py
Modificações:
- Removidas entradas interativas
- Adicionado melhor logging para Airflow
- Melhorado o tratamento de erros
- Adaptado para lidar corretamente com o contexto do Airflow
"""
import pandas as pd
import os
import sys
import logging
import argparse
from datetime import datetime
import io

# Add parent directory to path to allow importing utils
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("export_to_supabase_airflow")

# Import database modules with error handling
try:
    import psycopg2
    from dotenv import load_dotenv
except ImportError as e:
    logger.error(f"Pacote necessário em falta: {e}")
    logger.info("A instalar pacotes necessários...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "python-dotenv"])
    import psycopg2
    from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get database connection parameters
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 6543))  # Convert to integer
DB_NAME = os.getenv("DB_NAME")
DB_SCHEMA = os.getenv("DB_SCHEMA", "google_scraper")

def get_date_paths(specified_date=None):
    """Get appropriate date paths based on the specified date or current date"""
    if specified_date:
        try:
            dt = datetime.strptime(specified_date, "%Y-%m-%d")
            current_date = dt.strftime("%Y%m%d")
            current_year = dt.strftime("%Y")
            current_month = dt.strftime("%m")
            current_day = dt.strftime("%d")
        except ValueError as e:
            logger.error(f"Formato de data inválido: {e}")
            raise
    else:
        # Use current date
        dt = datetime.now()
        current_date = dt.strftime("%Y%m%d")
        current_year = dt.strftime("%Y")
        current_month = dt.strftime("%m")
        current_day = dt.strftime("%d")
    
    # Define paths with year/month/day structure
    structured_dir = os.path.join(PROJECT_ROOT, "data", "structured", current_year, current_month, current_day)
    
    # Ensure directory exists
    os.makedirs(structured_dir, exist_ok=True)
    
    # Input file
    victim_articles_csv = os.path.join(structured_dir, f"victim_articles_{current_date}.csv")
    
    return structured_dir, victim_articles_csv, current_date

def check_db_connection():
    """Check database connection and return connection object if successful"""
    logger.info(f"A ligar à base de dados {DB_NAME} em {DB_HOST}:{DB_PORT}")
    
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        logger.error("Parâmetros de ligação à base de dados em falta. Verifique as variáveis de ambiente.")
        return None
    
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        logger.info("Ligação à base de dados bem-sucedida")
        return conn
    except Exception as e:
        logger.error(f"Falha na ligação à base de dados: {e}")
        return None

def export_csv_to_table(conn, csv_file, table_name, schema=DB_SCHEMA):
    """Export CSV file to PostgreSQL/Supabase table using COPY"""
    logger.info(f"A exportar {csv_file} para a tabela {schema}.{table_name}")
    
    try:
        # Read CSV
        df = pd.read_csv(csv_file)
        logger.info(f"Lidas {len(df)} linhas do ficheiro CSV")
        
        # Create schema if it doesn't exist
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()
        
        # Prepare column definitions
        columns = df.columns.tolist()
        column_defs = []
        
        for col in columns:
            if col in ['id', 'title', 'original_url', 'resolved_url', 'domain', 'source', 'text', 'disaster_type', 'municipio', 'processed_date', 'event_type', 'processed_timestamp']:
                column_defs.append(f"{col} TEXT")
            elif col in ['fatalities', 'injuries', 'missing', 'affected']:
                column_defs.append(f"{col} INTEGER")
            else:
                column_defs.append(f"{col} TEXT")  # Default to TEXT for unknown columns
        
        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            {', '.join(column_defs)},
            PRIMARY KEY (id)
        );
        """
        
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()
            logger.info(f"Garantido que a tabela {schema}.{table_name} existe")
        
        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False, na_rep='NULL')
        csv_buffer.seek(0)
        
        # Use COPY command to insert data
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {schema}.{table_name} ({', '.join(columns)}) FROM STDIN WITH CSV", 
                csv_buffer
            )
            conn.commit()
            logger.info(f"Inseridas {len(df)} linhas em {schema}.{table_name}")
        
        return True
    except Exception as e:
        logger.error(f"Erro ao exportar CSV para a tabela: {e}")
        if conn:
            conn.rollback()
        return False

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Exportar dados para o Supabase (versão Airflow)")
    parser.add_argument("--date", type=str, help="Data específica para processar (AAAA-MM-DD)")
    
    args = parser.parse_args()
    
    logger.info("A iniciar a exportação para o Supabase (versão Airflow)")
    
    # Get appropriate paths
    structured_dir, victim_articles_csv, current_date = get_date_paths(args.date)
    
    # Check if victim articles CSV exists
    if not os.path.exists(victim_articles_csv):
        logger.error(f"CSV de artigos com vítimas não encontrado: {victim_articles_csv}")
        sys.exit(1)
    
    # Check database connection
    conn = check_db_connection()
    if not conn:
        logger.error("Falha na ligação à base de dados, a terminar")
        sys.exit(1)
    
    try:
        # Export data to database
        success = export_csv_to_table(
            conn,
            victim_articles_csv, 
            "disaster_articles"
        )
        
        if success:
            logger.info("Exportação de dados para o Supabase concluída com sucesso")
            return 0
        else:
            logger.error("Falha na exportação de dados para o Supabase")
            return 1
    finally:
        # Close connection
        if conn:
            conn.close()
            logger.info("Ligação à base de dados fechada")

if __name__ == "__main__":
    sys.exit(main())
