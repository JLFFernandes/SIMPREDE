import pandas as pd
import os
import psycopg2
import io
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Define paths relative to project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# 🔐 Carregar variáveis do .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 6543))  # Convert to integer
DB_NAME = os.getenv("DB_NAME")
DB_SCHEMA = os.getenv("DB_SCHEMA", "google_scraper")

def export_csv_to_table(csv_file, table_name, schema=DB_SCHEMA):
    """
    Export CSV file to PostgreSQL/Supabase table using COPY
    """
    print(f"⏳ Exportando {csv_file} para tabela {table_name}...")
    
    try:
        # Ler CSV
        print("🔍 Lendo arquivo CSV...")
        df = pd.read_csv(csv_file)
        print(f"✅ CSV lido com sucesso: {len(df)} linhas, {len(df.columns)} colunas")
        
        # Check if the DataFrame is empty (has no rows)
        if df.empty:
            print(f"⚠️ O arquivo CSV {csv_file} está vazio. Exportação para {table_name} cancelada.")
            return False
        
        # Check if the DataFrame has meaningful data (at least 1 row with non-null values)
        if df.dropna(how='all').empty:
            print(f"⚠️ O arquivo CSV {csv_file} não contém dados válidos. Exportação para {table_name} cancelada.")
            return False
        
        # Construir connection string para Supabase
        connection_string = (
            f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            f"?sslmode=require&connect_timeout=60"
        )
        print(f"🔌 Conectando ao banco de dados: {DB_HOST}:{DB_PORT}...")
        print(f"🔌 Usando connection string format (credenciais ocultas)...")
        
        # Abrir ligação psycopg2 usando connection string
        conn = psycopg2.connect(
            connection_string,
            sslmode="require",
            connect_timeout=30
        )
        print("✅ Conexão estabelecida com sucesso")
        cur = conn.cursor()

        # Preparar CSV em memória
        print("📝 Preparando dados para exportação...")
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # Obter nomes das colunas
        columns = ', '.join([f'"{col}"' for col in df.columns])
        print(f"📋 Colunas a serem criadas: {columns}")

        # Recriar tabela com colunas como TEXT
        print(f"🗑️ Removendo tabela {schema}.{table_name} se existir...")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
        
        col_defs = ', '.join([f'"{col}" TEXT' for col in df.columns])
        print(f"🏗️ Criando nova tabela {schema}.{table_name}...")
        cur.execute(f'CREATE TABLE {schema}.{table_name} ({col_defs})')

        # Verificar colunas criadas
        print("🔍 Verificando colunas criadas...")
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = '{schema}'")
        created_columns = [r[0] for r in cur.fetchall()]
        print("📋 Colunas criadas na tabela:", created_columns)

        # Importar os dados via COPY
        print("📤 Iniciando COPY dos dados...")
        copy_sql = f'COPY {schema}.{table_name} ({columns}) FROM STDIN WITH (FORMAT CSV)'
        cur.copy_expert(copy_sql, buffer)
        print("✅ COPY concluído com sucesso")

        print("💾 Fazendo commit das alterações...")
        conn.commit()
        cur.close()
        conn.close()
        print("🔌 Conexão fechada")

        print(f"✅ Dados exportados com sucesso para '{table_name}' via COPY.")
        return True
    except Exception as e:
        print(f"❌ Erro ao exportar via COPY: {e}")
        # Print full stack trace for debugging
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Exporta dados CSV para Supabase/PostgreSQL")
    parser.add_argument(
        "--tipo", 
        choices=["artigos_filtrados", "all"],
        default="all",
        help="Tipo de dados a exportar"
    )
    parser.add_argument(
        "--debug", 
        action="store_true",
        help="Ativar modo de depuração com informações adicionais"
    )
    args = parser.parse_args()
    
    # Print DB connection info in debug mode
    if args.debug:
        print(f"🔧 Informações de conexão:")
        print(f"  Host: {DB_HOST}")
        print(f"  Port: {DB_PORT}")
        print(f"  User: {DB_USER}")
        print(f"  Database: {DB_NAME}")
        print(f"  Schema: {DB_SCHEMA}")
    
    # Get current date for file naming
    current_date = datetime.now().strftime("%Y%m%d")
    current_year = datetime.now().strftime("%Y")
    current_month = datetime.now().strftime("%m")
    current_day = datetime.now().strftime("%d")
    
    # Configuração dos arquivos CSV e suas tabelas correspondentes
    csv_configs = []
    
    if args.tipo in ["artigos_filtrados", "all"]:
        # Look for the file in the year/month/day structure
        year_month_day_path = os.path.join(
            PROJECT_ROOT, "data", "structured", current_year, current_month, current_day, 
            f"artigos_filtrados_{current_date}.csv"
        )
        
        csv_configs.append({
            "file": year_month_day_path,
            "table": f"artigos_filtrados_{current_date}_staging"
        })
    
    # Processar cada arquivo CSV
    for config in csv_configs:
        # Check if file exists
        file_to_use = config.get("file")
        
        print(f"🔍 Verificando arquivo: {file_to_use}")
        if not os.path.exists(file_to_use):
            print(f"  ❌ Arquivo não encontrado: {file_to_use}")
            continue
        
        print(f"  ✅ Arquivo encontrado: {file_to_use}")
        # Check file size
        file_size = os.path.getsize(file_to_use)
        print(f"  📊 Tamanho do arquivo: {file_size} bytes")
        
        # Skip empty files
        if file_size <= 10:  # Just a header or empty file
            print(f"  ⚠️ Arquivo vazio ou apenas com cabeçalho. Ignorando.")
            continue
            
        # Check if file is readable
        try:
            with open(file_to_use, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                second_line = f.readline()
                print(f"  ✅ Arquivo pode ser lido. Primeira linha: {first_line[:50]}...")
                
                # Check if there's data beyond the header
                if not second_line or second_line.strip() == "":
                    print(f"  ⚠️ Arquivo contém apenas cabeçalho sem dados. Ignorando.")
                    continue
                    
        except Exception as e:
            print(f"  ⚠️ Aviso: Não foi possível ler o arquivo: {e}")
            continue
        
        # Export the file to Supabase
        export_csv_to_table(file_to_use, config["table"])

if __name__ == "__main__":
    main()
