import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import psycopg2
import io

# Define paths relative to project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CSV_FILE = os.path.join(PROJECT_ROOT, "data", "structured", "artigos_filtrados.csv")

# 🔐 Carregar variáveis do .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")
TABLE_NAME = "artigos_municipios_pt_staging"

# Ler CSV
df = pd.read_csv(CSV_FILE)

try:
    print("⏳ A exportar com psycopg2 e COPY...")

    # Abrir ligação psycopg2
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        sslmode='require'
    )
    cur = conn.cursor()

    # Preparar CSV em memória
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # Obter nomes das colunas
    columns = ', '.join([f'"{col}"' for col in df.columns])

    # Recriar tabela com colunas como TEXT
    cur.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{TABLE_NAME}")
    col_defs = ', '.join([f'"{col}" TEXT' for col in df.columns])
    cur.execute(f'CREATE TABLE {DB_SCHEMA}.{TABLE_NAME} ({col_defs})')

    # Verificar colunas criadas
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TABLE_NAME}' AND table_schema = '{DB_SCHEMA}'")
    created_columns = [r[0] for r in cur.fetchall()]
    print("📋 Colunas criadas na tabela:", created_columns)

    # Importar os dados via COPY
    copy_sql = f'COPY {DB_SCHEMA}.{TABLE_NAME} ({columns}) FROM STDIN WITH (FORMAT CSV)'
    cur.copy_expert(copy_sql, buffer)

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Dados exportados com sucesso para '{TABLE_NAME}' via COPY.")

except Exception as e:
    print(f"❌ Erro ao exportar via COPY: {e}")