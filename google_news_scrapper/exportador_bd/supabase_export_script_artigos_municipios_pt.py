import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# 🔐 Carregar variáveis do .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
CSV_FILE = "data/artigos_google_municipios_pt.csv"
TABLE_NAME = "artigos_municipios_pt"

# Criar engine de ligação
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Ler CSV
df = pd.read_csv(CSV_FILE)

# Exportar para a tabela (cria se não existir)
df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

print(f"✅ CSV importado com sucesso para a tabela '{TABLE_NAME}'!")