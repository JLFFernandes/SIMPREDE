# File: airflow-GOOGLE-NEWS-SCRAPER/scripts/google_scraper/exportador_bd/export_to_supabase_airflow.py
# Script para exportar artigos filtrados para a base de dados Supabase
# Este script é executado como parte do DAG do Airflow e deve ser compatível com o ambiente do Airflow

import sys
import os
import logging
import argparse
import pandas as pd
import psycopg2
import re
from dotenv import load_dotenv, find_dotenv
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

# Load environment variables from the project root
load_dotenv(find_dotenv())

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
    """Obter configuração da base de dados a partir de variáveis de ambiente ou ficheiro .env"""
    # Primeiro tentar obter das variáveis de ambiente (contentor Airflow)
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '6543'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'sslmode': os.getenv('DB_SSLMODE', 'require'),
        'schema': os.getenv('DB_SCHEMA', 'google_scraper')  # 🎯 ESQUEMA PADRÃO: 'google_scraper'
    }
    
    log_progress("🔍 Verificação inicial do ambiente...")
    log_progress(f"  DB_HOST do ambiente: {'ENCONTRADO' if db_config['host'] else 'NÃO_ENCONTRADO'}")
    log_progress(f"  DB_USER do ambiente: {'ENCONTRADO' if db_config['user'] else 'NÃO_ENCONTRADO'}")
    log_progress(f"  DB_PASSWORD do ambiente: {'ENCONTRADO' if db_config['password'] else 'NÃO_ENCONTRADO'}")
    
    # Se não encontrado no ambiente, tentar carregar do ficheiro .env
    if not db_config['host'] or not db_config['user'] or not db_config['password']:
        log_progress("🔍 Configuração da base de dados não encontrada no ambiente, procurando ficheiro .env...")
        
        # Look for .env file only at project root
        possible_env_paths = [
            Path(__file__).resolve().parents[4] / '.env'
        ]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_paths = []
        for path in possible_env_paths:
            abs_path = os.path.abspath(path)
            if abs_path not in seen:
                seen.add(abs_path)
                unique_paths.append(abs_path)
        
        env_file_found = False
        log_progress(f"🔍 Verificando {len(unique_paths)} localizações possíveis do .env...")
        
        for env_path in unique_paths:
            log_progress(f"  A verificar: {env_path}")
            if os.path.exists(env_path):
                log_progress(f"✅ Ficheiro .env encontrado em: {env_path}")
                try:
                    # Parser simples do .env com melhor tratamento de erros
                    with open(env_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        log_progress(f"📄 Tamanho do ficheiro .env: {len(content)} caracteres")
                        
                        for line_num, line in enumerate(content.splitlines(), 1):
                            line = line.strip()
                            if line and not line.startswith('#') and '=' in line:
                                try:
                                    key, value = line.split('=', 1)
                                    key = key.strip()
                                    value = value.strip()
                                    
                                    # Remover aspas se presentes
                                    if value.startswith('"') and value.endswith('"'):
                                        value = value[1:-1]
                                    elif value.startswith("'") and value.endswith("'"):
                                        value = value[1:-1]
                                    
                                    if key == 'DB_HOST' and not db_config['host']:
                                        db_config['host'] = value
                                        log_progress(f"  ✅ Definido DB_HOST do .env")
                                    elif key == 'DB_PORT':
                                        db_config['port'] = value
                                        log_progress(f"  ✅ Definido DB_PORT do .env")
                                    elif key == 'DB_NAME':
                                        db_config['database'] = value
                                        log_progress(f"  ✅ Definido DB_NAME do .env")
                                    elif key == 'DB_USER' and not db_config['user']:
                                        db_config['user'] = value
                                        log_progress(f"  ✅ Definido DB_USER do .env")
                                    elif key == 'DB_PASSWORD' and not db_config['password']:
                                        db_config['password'] = value
                                        log_progress(f"  ✅ Definido DB_PASSWORD do .env")
                                    elif key == 'DB_SSLMODE':
                                        db_config['sslmode'] = value
                                        log_progress(f"  ✅ Definido DB_SSLMODE do .env")
                                    elif key == 'DB_SCHEMA':
                                        db_config['schema'] = value
                                        log_progress(f"  ✅ Definido DB_SCHEMA do .env")
                                except Exception as line_error:
                                    log_progress(f"  ⚠️ Erro ao analisar linha {line_num}: {line_error}", "warning")
                                    continue
                    
                    env_file_found = True
                    break
                    
                except Exception as e:
                    log_progress(f"⚠️ Erro ao ler ficheiro .env {env_path}: {e}", "warning")
                    continue
            else:
                log_progress(f"  ❌ Não encontrado: {env_path}")
        
        if not env_file_found:
            log_progress("⚠️ Nenhum ficheiro .env encontrado em qualquer localização", "warning")
    
    # Verificação final da configuração
    log_progress("🔍 Verificação final da configuração...")
    log_progress(f"  DB_HOST: {'DEFINIDO' if db_config['host'] else 'EM_FALTA'}")
    log_progress(f"  DB_USER: {'DEFINIDO' if db_config['user'] else 'EM_FALTA'}")
    log_progress(f"  DB_PASSWORD: {'DEFINIDO' if db_config['password'] else 'EM_FALTA'}")
    log_progress(f"  DB_PORT: {db_config['port']}")
    log_progress(f"  DB_NAME: {db_config['database']}")
    log_progress(f"  DB_SCHEMA: {db_config['schema']}")
    
    # Validar campos obrigatórios
    required_fields = ['host', 'user', 'password']
    missing_fields = [field for field in required_fields if not db_config[field]]
    
    if missing_fields:
        log_progress(f"❌ Configuração obrigatória da base de dados em falta: {missing_fields}", "error")
        log_progress("Variáveis de ambiente disponíveis:", "debug")
        for key in ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_SSLMODE', 'DB_SCHEMA']:
            value = os.getenv(key, 'NÃO_DEFINIDO')
            log_progress(f"  {key}: {'***' if 'PASSWORD' in key and value != 'NÃO_DEFINIDO' else value}", "debug")
        
        # Tentar mais uma vez com injeção direta de variáveis de ambiente
        log_progress("🔄 Tentativa de configuração direta de variáveis de ambiente do .env...")
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
                
                # Repetir carregamento da configuração
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
                    log_progress("✅ Configuração da base de dados carregada após injeção no ambiente")
                else:
                    log_progress(f"❌ Ainda em falta após injeção: {missing_fields}", "error")
            
        except Exception as inject_error:
            log_progress(f"⚠️ Falha na injeção no ambiente: {inject_error}", "warning")
        
        if missing_fields:
            raise ValueError(f"Configuração obrigatória da base de dados em falta: {missing_fields}")
    
    log_progress(f"✅ Configuração da base de dados carregada: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    return db_config

def find_filtered_articles_file(target_date, input_file=None):
    """Encontrar o ficheiro de artigos filtrados da tarefa anterior com suporte para caminhos controlados"""
    if input_file and os.path.exists(input_file):
        log_progress(f"✅ A usar ficheiro de entrada fornecido: {input_file}")
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
            log_progress(f"✅ Ficheiro encontrado: {path} ({file_size} bytes)")
            found_files.append((path, file_size))
    
    if not found_files:
        log_progress(f"ℹ️ Nenhum ficheiro de artigos filtrados encontrado para {date_suffix}. Isto é normal quando não foram detetados eventos relacionados com desastres.")
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
    log_progress(f"📋 Ficheiro selecionado para exportação: {selected_file}")
    
    return selected_file

def convert_date_format(date_str):
    """Converter data para formato DD/MM/YYYY para manter consistência com o resto da BD"""
    if pd.isna(date_str) or not date_str:
        return None
    
    try:
        # Handle different possible date formats
        date_str = str(date_str).strip()
        
        # If already in DD/MM/YYYY format, return as is
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
            # Validate that it's not a future date (likely wrong format)
            try:
                day, month, year = map(int, date_str.split('/'))
                test_date = datetime(year, month, day)
                # If date is more than 1 day in the future, likely wrong format
                if test_date > datetime.now() + timedelta(days=1):
                    log_progress(f"⚠️ Data futura detectada: {date_str}, pode estar em formato incorreto", "warning")
            except:
                pass
            return date_str
        
        # If in DD-MM-YYYY format, convert to DD/MM/YYYY
        if re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', date_str):
            day, month, year = date_str.split('-')
            return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
        
        # If in YYYY-MM-DD format, convert to DD/MM/YYYY
        if re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', date_str):
            year, month, day = date_str.split('-')
            # Validate the date makes sense
            try:
                test_date = datetime(int(year), int(month), int(day))
                # Check if this creates a future date (indicating correct parsing)
                if test_date > datetime.now() + timedelta(days=1):
                    log_progress(f"⚠️ YYYY-MM-DD gerou data futura: {date_str} -> {day.zfill(2)}/{month.zfill(2)}/{year}", "warning")
                return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
            except ValueError as ve:
                log_progress(f"⚠️ Data inválida detectada: {date_str} - {ve}", "warning")
                return None
        
        # If in YYYY/MM/DD format, convert to DD/MM/YYYY
        if re.match(r'^\d{4}/\d{1,2}/\d{1,2}$', date_str):
            year, month, day = date_str.split('/')
            return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
        
        # Try parsing with pandas and convert to DD/MM/YYYY
        parsed_date = pd.to_datetime(date_str, errors='coerce')
        if not pd.isna(parsed_date):
            # Check if parsed date is reasonable (not too far in future)
            if parsed_date > datetime.now() + timedelta(days=1):
                log_progress(f"⚠️ Data futura após parsing: {date_str} -> {parsed_date.strftime('%d/%m/%Y')}", "warning")
            return parsed_date.strftime('%d/%m/%Y')
        
        log_progress(f"⚠️ Não foi possível analisar data: {date_str}", "warning")
        return None
        
    except Exception as e:
        log_progress(f"⚠️ Erro ao converter data '{date_str}': {e}", "warning")
        return None

def create_table_if_not_exists(cursor, schema, table_name, df_columns):
    """Criar a tabela se não existir com colunas dinâmicas baseadas nos dados de entrada"""
    # Eliminar a tabela primeiro para garantir esquema limpo (pois estamos a lidar com tabelas temporárias)
    drop_table_sql = f"DROP TABLE IF EXISTS {schema}.{table_name};"
    cursor.execute(drop_table_sql)
    log_progress(f"🗑️ Tabela existente {schema}.{table_name} eliminada se existia")
    
    # Generate column definitions based on input DataFrame
    column_defs = []
    for col in df_columns:
        # Handle various ID column names - preserve original ID if possible
        if col.upper() in ['ID', 'ORIGINAL_ID', 'GOOGLE_ID']:
            column_defs.append(f"{col} TEXT PRIMARY KEY")
        elif col in ['fatalities', 'injured', 'evacuated', 'displaced', 'missing', 'year', 'month', 'day']:
            column_defs.append(f"{col} INTEGER DEFAULT 0")
        elif col == 'relevance_score':
            column_defs.append(f"{col} REAL DEFAULT 0")
        elif col == 'date':
            # Use TEXT to store DD/MM/YYYY format consistently with rest of DB
            column_defs.append(f"{col} TEXT")
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
    log_progress(f"✅ Tabela {schema}.{table_name} criada com esquema correto")
    
    # Adicionar índices
    if 'date' in df_columns:
        cursor.execute(f"CREATE INDEX idx_{table_name}_date ON {schema}.{table_name}(date);")
    if 'district' in df_columns:
        cursor.execute(f"CREATE INDEX idx_{table_name}_district ON {schema}.{table_name}(district);")
    if 'evento_nome' in df_columns:
        cursor.execute(f"CREATE INDEX idx_{table_name}_evento ON {schema}.{table_name}(evento_nome);")
    
    log_progress(f"✅ Índices criados para a tabela {schema}.{table_name}")

def prepare_dataframe_for_insert(df):
    """Preparar o dataframe para inserção na base de dados com tipos de dados adequados"""
    log_progress("🔧 A preparar dataframe para inserção na base de dados...")
    
    # Create a copy to avoid modifying the original
    df_prepared = df.copy()
    
    # Debug: Log original ID column info
    id_columns = [col for col in df_prepared.columns if 'id' in col.lower()]
    log_progress(f"🔍 Colunas de ID encontradas: {id_columns}")
    
    # If there's an original_id or google_id column, prefer that over evt_ IDs
    if 'original_id' in df_prepared.columns:
        log_progress("✅ A usar original_id como ID principal")
        df_prepared['ID'] = df_prepared['original_id']
    elif 'google_id' in df_prepared.columns:
        log_progress("✅ A usar google_id como ID principal")
        df_prepared['ID'] = df_prepared['google_id']
    elif 'ID' not in df_prepared.columns and 'id' in df_prepared.columns:
        log_progress("✅ A renomear coluna 'id' para 'ID'")
        df_prepared['ID'] = df_prepared['id']
    
    # Log sample of ID values for debugging
    if 'ID' in df_prepared.columns:
        sample_ids = df_prepared['ID'].head(3).tolist()
        log_progress(f"🔍 Amostra de IDs: {sample_ids}")
    
    # Converter coluna de data para formato adequado
    if 'date' in df_prepared.columns:
        log_progress("📅 A converter formatos de data...")
        # Log some sample dates before conversion
        sample_dates = df_prepared['date'].head(3).tolist()
        log_progress(f"📅 Datas originais (amostra): {sample_dates}")
        
        df_prepared['date'] = df_prepared['date'].apply(convert_date_format)
        
        # Log converted dates
        sample_converted = df_prepared['date'].head(3).tolist()
        log_progress(f"📅 Datas convertidas (amostra): {sample_converted}")
        
        # Remover linhas com datas inválidas
        invalid_dates = df_prepared['date'].isna()
        if invalid_dates.any():
            log_progress(f"⚠️ A remover {invalid_dates.sum()} linhas com datas inválidas", "warning")
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
    
    log_progress(f"✅ Preparadas {len(df_prepared)} linhas para inserção")
    return df_prepared

def insert_articles(cursor, schema, table_name, articles_df):
    """Inserir artigos na base de dados"""
    if articles_df.empty:
        log_progress("⚠️ Nenhum artigo para inserir", "warning")
        return 0
    
    # Preparar o dataframe primeiro
    df_prepared = prepare_dataframe_for_insert(articles_df)
    
    if df_prepared.empty:
        log_progress("⚠️ Nenhum artigo válido para inserir após preparação", "warning")
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
            log_progress(f"⚠️ Erro ao inserir artigo {idx}: {e}", "warning")
            if error_count <= 3:  # Only show first 3 errors to avoid spam
                log_progress(f"   Dados da linha: {dict(row[columns])}", "debug")
            continue
    
    if error_count > 3:
        log_progress(f"⚠️ ... e mais {error_count - 3} erros", "warning")
    
    log_progress(f"✅ Inseridos/atualizados {inserted_count} artigos (falharam: {error_count})")
    return inserted_count

def save_export_statistics(output_dir, date_str, stats):
    """Guardar estatísticas de exportação em caminhos de saída controlados"""
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
        
        log_progress(f"✅ Estatísticas de exportação guardadas: {stats_file}")
        
        # Também guardar um CSV de backup para inspeção manual
        if 'exported_count' in stats and stats['exported_count'] > 0:
            backup_file = os.path.join(output_dir, f"export_backup_{date_str.replace('-', '')}.csv")
            log_progress(f"📋 Localização de backup de exportação preparada: {backup_file}")
            
    except Exception as e:
        log_progress(f"⚠️ Não foi possível guardar estatísticas de exportação: {e}", "warning")

def export_to_supabase(target_date=None, input_file=None, output_dir=None, date_str=None):
    """Função principal de exportação com suporte para caminhos controlados"""
    if not target_date:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    # Use provided date_str or generate from target_date
    if not date_str:
        date_str = target_date
    
    # Generate table name with compact date format: artigos_filtrados_20250605_staging
    date_compact = date_str.replace('-', '')
    table_name = f"artigos_filtrados_{date_compact}_staging"
    
    log_progress(f"🚀 A iniciar exportação para Supabase para a data: {target_date}")
    log_progress(f"📋 Tabela de destino: {table_name}")
    
    if output_dir:
        log_progress(f"📁 A usar diretório de saída controlado: {output_dir}")
    
    # Encontrar o ficheiro de artigos filtrados com suporte para caminhos controlados
    input_file_path = find_filtered_articles_file(target_date, input_file)
    if not input_file_path:
        log_progress("ℹ️ Nenhum ficheiro de artigos filtrados encontrado. Isto normalmente significa que não foram detetados eventos relacionados com desastres para esta data.")
        log_progress("✅ Tarefa de exportação concluída com sucesso - nenhum dado para exportar.")
        
        # Guardar estatísticas indicando que não havia dados disponíveis
        if output_dir:
            save_export_statistics(output_dir, date_str, {
                "input_file": None,
                "exported_count": 0,
                "total_input_rows": 0,
                "table_name": table_name,
                "status": "sem_dados_disponiveis",
                "message": "Nenhum artigo relacionado com desastres encontrado para esta data"
            })
        
        return 0  # Retornar sucesso (0) em vez de lançar exceção
    
    # Carregar artigos
    try:
        log_progress(f"📂 A carregar artigos de: {input_file_path}")
        df = pd.read_csv(input_file_path)
        log_progress(f"📊 Carregados {len(df)} artigos com colunas: {list(df.columns)}")
        
        # Determinar tipo de ficheiro para registo
        if 'vitimas_filtrados' in input_file_path:
            file_type = "artigos com vítimas"
        elif 'sem_vitimas' in input_file_path:
            file_type = "artigos sem vítimas (mas relacionados com desastres)"
        elif 'municipios_pt' in input_file_path:
            file_type = "todos os artigos relevantes relacionados com desastres"
        else:
            file_type = "artigos filtrados"
        
        log_progress(f"📋 A processar {file_type}")
        
        if df.empty:
            log_progress("ℹ️ Ficheiro de artigos está vazio - nenhum evento relacionado com desastres detetado.")
            log_progress("✅ Tarefa de exportação concluída com sucesso - nenhum dado para exportar.")
            # Guardar estatísticas de exportação vazia
            if output_dir:
                save_export_statistics(output_dir, date_str, {
                    "input_file": input_file_path,
                    "exported_count": 0,
                    "total_input_rows": 0,
                    "table_name": table_name,
                    "file_type": file_type,
                    "status": "sem_dados_ficheiro_vazio",
                    "message": "Ficheiro de entrada estava vazio - nenhum artigo relacionado com desastres encontrado"
                })
            return 0
        
    except Exception as e:
        log_progress(f"❌ Erro ao carregar ficheiro de artigos: {e}", "error")
        raise
    
    # Obter configuração da base de dados
    try:
        db_config = get_database_config()
    except Exception as e:
        log_progress(f"❌ Erro de configuração da base de dados: {e}", "error")
        raise
    
    # Conectar à base de dados e exportar
    connection = None
    try:
        log_progress(f"🔗 A conectar à base de dados...")
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
        
        log_progress(f"✅ Conectado à base de dados com sucesso")
        
        # Criar tabela se não existir com colunas dinâmicas
        create_table_if_not_exists(cursor, db_config['schema'], table_name, df.columns)
        
        # Inserir artigos
        inserted_count = insert_articles(cursor, db_config['schema'], table_name, df)
        
        # Criar CSV de backup no diretório de saída
        backup_file = None
        if output_dir:
            try:
                os.makedirs(output_dir, exist_ok=True)
                backup_file = os.path.join(output_dir, f"export_backup_{date_str.replace('-', '')}.csv")
                df.to_csv(backup_file, index=False)
                log_progress(f"💾 Backup de exportação guardado: {backup_file}")
            except Exception as e:
                log_progress(f"⚠️ Não foi possível guardar CSV de backup: {e}", "warning")
        
        # Guardar estatísticas de exportação em caminhos controlados
        export_stats = {
            "input_file": input_file_path,
            "exported_count": inserted_count,
            "total_input_rows": len(df),
            "table_name": f"{db_config['schema']}.{table_name}",
            "database_host": db_config['host'],
            "file_type": file_type,
            "backup_file": backup_file,
            "columns": list(df.columns),
            "status": "sucesso"
        }
        
        if output_dir:
            save_export_statistics(output_dir, date_str, export_stats)
        
        log_progress(f"✅ Exportação concluída com sucesso. Inseridos/atualizados {inserted_count} {file_type} na tabela {table_name}")
        return inserted_count
        
    except Exception as e:
        log_progress(f"❌ Operação de base de dados falhou: {e}", "error")
        
        # Guardar estatísticas de erro
        if output_dir:
            save_export_statistics(output_dir, date_str, {
                "input_file": input_file_path if 'input_file_path' in locals() else input_file,
                "exported_count": 0,
                "table_name": table_name,
                "error": str(e),
                "status": "falhado"
            })
        
        raise
    finally:
        if connection:
            connection.close()
            log_progress("🔗 Ligação à base de dados fechada")

def main():
    """Função principal com suporte para caminhos de saída controlados"""
    parser = argparse.ArgumentParser(description="Exportar artigos filtrados para Supabase (versão Airflow)")
    parser.add_argument("--date", type=str, help="Data alvo (AAAA-MM-DD)")
    parser.add_argument("--input_file", type=str, help="Caminho do ficheiro de entrada específico")
    parser.add_argument("--output_dir", type=str, help="Diretório de saída para registos e estatísticas de exportação")
    parser.add_argument("--date_str", type=str, help="String de data para nomeação de ficheiros")
    parser.add_argument("--debug", action="store_true", help="Ativar registo de depuração")
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        log_progress("🔍 REGISTO DE DEPURAÇÃO ativado", "debug")
    
    log_progress("A iniciar export_to_supabase_airflow")
    log_progress(f"Parâmetros: date={args.date}")
    log_progress(f"Caminhos: input_file={args.input_file}, output_dir={args.output_dir}, date_str={args.date_str}")
    
    try:
        result = export_to_supabase(
            target_date=args.date,
            input_file=args.input_file,
            output_dir=args.output_dir,
            date_str=args.date_str
        )
        log_progress(f"✅ Exportação concluída com sucesso. Processados {result} artigos")
        return 0
    except Exception as e:
        log_progress(f"❌ Exportação falhou: {e}", "error")
        import traceback
        log_progress(f"❌ Traceback completo: {traceback.format_exc()}", "error")
        return 1

