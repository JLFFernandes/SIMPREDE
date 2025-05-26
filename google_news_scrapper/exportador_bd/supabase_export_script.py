import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
import io

# 🔐 Carregar variáveis do .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

# Configuração dos arquivos CSV e suas tabelas correspondentes
CSV_CONFIGS = [
    {
        "file": "data/structured/artigos_cnnportugal.csv",
        "table": "artigos_cnnportugal_staging"
    },
    {
        "file": "data/structured/artigos_jn.csv",
        "table": "artigos_jn_staging"
    },
    {
        "file": "data/structured/artigos_sicnoticias.csv",
        "table": "artigos_sicnoticias_staging"
    },
    {
        "file": "data/structured/artigos_publico.csv",
        "table": "artigos_publico_staging"
    }
]

def main():


    # Processar cada arquivo CSV
    for config in CSV_CONFIGS:
        try:
            # Ler CSV
            df = pd.read_csv(config["file"])

            print(f"⏳ A exportar {config['file']} com psycopg2 e COPY...")

            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                sslmode='require'
            )
            cur = conn.cursor()

            # Converter DataFrame para CSV em memória
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            # Obter colunas e preparar CREATE TABLE
            columns = ', '.join([f'"{col}"' for col in df.columns])
            col_defs = ', '.join([f'"{col}" TEXT' for col in df.columns])

            # Criar tabela
            cur.execute(f'DROP TABLE IF EXISTS {DB_SCHEMA}.{config["table"]}')
            cur.execute(f'CREATE TABLE {DB_SCHEMA}.{config["table"]} ({col_defs})')

            # Verificar colunas
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{config['table']}' AND table_schema = '{DB_SCHEMA}'")
            created_columns = [r[0] for r in cur.fetchall()]
            print("📋 Colunas criadas:", created_columns)

            # Executar COPY
            copy_sql = f'COPY {DB_SCHEMA}.{config["table"]} ({columns}) FROM STDIN WITH (FORMAT CSV)'
            cur.copy_expert(copy_sql, buffer)

            conn.commit()
            cur.close()
            conn.close()

            print(f"✅ CSV {config['file']} importado com sucesso para '{config['table']}' via COPY.")

        except Exception as e:
            print(f"❌ Erro ao processar {config['file']}: {str(e)}")

if __name__ == "__main__":
    main()
