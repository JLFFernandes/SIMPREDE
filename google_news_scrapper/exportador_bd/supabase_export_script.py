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
    # Criar engine de ligação
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # Processar cada arquivo CSV
    for config in CSV_CONFIGS:
        try:
            # Ler CSV
            df = pd.read_csv(config["file"])
            
            # Exportar para a tabela
            df.to_sql(
                config["table"], 
                engine, 
                if_exists="replace", 
                index=False, 
                schema=DB_SCHEMA
            )
            
            print(f"✅ CSV {config['file']} importado com sucesso para a tabela '{config['table']}'!")
            
        except Exception as e:
            print(f"❌ Erro ao processar {config['file']}: {str(e)}")

if __name__ == "__main__":
    main()
