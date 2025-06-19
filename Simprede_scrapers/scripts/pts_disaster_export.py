import os
import pandas as pd
import psycopg2
from datetime import date

# -----------------------------
# Caminho do ficheiro Excel
# -----------------------------
EXCEL_PATH = "/usr/local/airflow/include/pts_disaster_export.xlsx"

# -----------------------------
# Parâmetros de conexão ao Supabase
# -----------------------------
DB_PARAMS = {
    "host": "aws-0-eu-west-3.pooler.supabase.com",
    "port": "6543",
    "dbname": "postgres",
    "user": "postgres.kyrfsylobmsdjlrrpful",
    "password": "HXU3tLVVXRa1jtjo",
    "sslmode": "require"
}

# -----------------------------
# Funções auxiliares
# -----------------------------
def safe_int(x):
    try:
        xi = int(float(x))
        return xi if xi < 2_147_483_647 else 2_147_483_647
    except:
        return 0

def safe_str(x, max_len=100):
    try:
        s = str(x)
        return s[:max_len]
    except:
        return ""

def parse_date(val):
    if pd.isnull(val):
        return None
    try:
        return pd.to_datetime(val, dayfirst=True).date()
    except:
        return None

# -----------------------------
# Verificação inicial
# -----------------------------
if not os.path.exists(EXCEL_PATH):
    raise FileNotFoundError(f"Excel não encontrado: {EXCEL_PATH}")

# -----------------------------
# Carregar Excel
# -----------------------------
df = pd.read_excel(EXCEL_PATH)
df.columns = df.columns.str.strip().str.lower()
df.rename(columns={"municipali": "municipality"}, inplace=True)
print(f"Linhas lidas do Excel: {len(df)}")

# -----------------------------
# Conexão à base de dados
# -----------------------------
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()
print("Ligação estabelecida ao Supabase")

# -----------------------------
# Inserção dos dados
# -----------------------------
for index, row in df.iterrows():
    try:
        ev_date = parse_date(row.get("date"))
        if ev_date is None:
            y = safe_int(row.get("year")) or 1900
            m = safe_int(row.get("month")) or 1
            d = safe_int(row.get("day")) or 1
            try:
                ev_date = date(y, m, d)
            except:
                ev_date = date(1900, 1, 1)

        # Inserir disaster
        cur.execute(
            """
            INSERT INTO disasters (
                type, subtype, date, year, month, day, hour
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                safe_str(row.get("type")),
                safe_str(row.get("subtype")),
                ev_date,
                safe_int(row.get("year")),
                safe_int(row.get("month")),
                safe_int(row.get("day")),
                safe_str(row.get("hour"))
            )
        )
        disaster_id = cur.fetchone()[0]

        # Usar latitude e longitude diretamente do Excel
        lat = float(row.get("latitude")) if pd.notnull(row.get("latitude")) else 0
        lon = float(row.get("longitude")) if pd.notnull(row.get("longitude")) else 0

        # Inserir localização
        cur.execute(
            """
            INSERT INTO location (
                id, latitude, longitude, georef_class,
                district, municipality, parish, dicofreg, geom
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            )
            """,
            (
                disaster_id,
                lat, lon,
                safe_int(row.get("georef")) or 1,
                safe_str(row.get("district")),
                safe_str(row.get("municipality")),
                safe_str(row.get("parish")),
                safe_str(row.get("dicofreg")),
                lon, lat
            )
        )

        # Inserir impactos humanos
        cur.execute(
            """
            INSERT INTO human_impacts (
                id, fatalities, injured, evacuated, displaced, missing
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                disaster_id,
                safe_int(row.get("fatalities")),
                safe_int(row.get("injured")),
                safe_int(row.get("evacuated")),
                safe_int(row.get("displaced")),
                safe_int(row.get("missing"))
            )
        )

        # Inserir fontes de informação
        cur.execute(
            """
            INSERT INTO information_sources (
                disaster_id, source_name, source_date, source_type, page
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                disaster_id,
                safe_str(row.get("source")),
                parse_date(row.get("sourcedate")),
                safe_str(row.get("sourcetype")),
                safe_str(row.get("page"))
            )
        )

    except Exception as e:
        print(f"[Erro na linha {index + 2}]: {e}")
        conn.rollback()

# -----------------------------
# Fechar ligação
# -----------------------------
conn.commit()
cur.close()
conn.close()
print("Importação concluída e ligação encerrada.")
