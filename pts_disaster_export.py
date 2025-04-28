import os
import pandas as pd
import psycopg2
from datetime import date

# -----------------------------
# Configuração do ficheiro Excel
# -----------------------------
EXCEL_PATH = "/Users/joseferreirafernandes/Desktop/5 ano/Projeto/fase 2/doc/pts_disaster_export.xlsx"

# -----------------------------
# Parâmetros de conexão ao Supabase (Transaction Pooler IPv4)
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
    """Converte valores para inteiro, respeitando limites do INTEGER"""
    try:
        xi = int(float(x))
        return xi if xi < 2_147_483_647 else 2_147_483_647
    except:
        return 0


def safe_str(x, max_len=100):
    """Converte valores para string e trunca com base no comprimento máximo"""
    try:
        s = str(x)
        return s if len(s) <= max_len else s[:max_len]
    except:
        return ""


def parse_geom(geom_str):
    """Extrai latitude e longitude de strings no formato 'POINT(lon lat)'"""
    try:
        if pd.isna(geom_str) or not isinstance(geom_str, str):
            return None, None
        g = geom_str.strip()
        if g.startswith("POINT(") and g.endswith(")"):
            lon, lat = map(float, g[6:-1].split())
            return lat, lon
    except:
        pass
    return None, None


def parse_date(val):
    """Converte vários formatos de data em objetos date"""
    if pd.isnull(val):
        return None
    try:
        if isinstance(val, str) and "/" in val:
            return pd.to_datetime(val, dayfirst=True).date()
        return pd.to_datetime(val).date()
    except:
        return None

# -----------------------------
# Verificações iniciais
# -----------------------------
if not os.path.exists(EXCEL_PATH):
    raise FileNotFoundError(f"Excel não encontrado: {EXCEL_PATH}")

# -----------------------------
# Abre ligação ao Supabase
# -----------------------------
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()
print("Ligação estabelecida ao Supabase")

# -----------------------------
# Importa Excel de desastres
# -----------------------------
df = pd.read_excel(EXCEL_PATH)
print(f"Linhas lidas do Excel: {len(df)}")

for _, row in df.iterrows():
    # Constrói a data do evento
    ev_date = parse_date(row.get("date"))
    if ev_date is None:
        y = safe_int(row.get("year")) or 1900
        m = safe_int(row.get("month")) or 1
        d = safe_int(row.get("day")) or 1
        try:
            ev_date = date(y, m, d)
        except:
            ev_date = date(1900, 1, 1)

    # Insere na tabela disasters
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

    # Insere na tabela location
    lat, lon = parse_geom(row.get("geom"))
    georef_cls = safe_int(row.get("georef")) or 1
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
            lat or 0, lon or 0,
            georef_cls,
            safe_str(row.get("district")),
            safe_str(row.get("municipali")),
            safe_str(row.get("parish")),
            safe_str(row.get("dicofreg")),
            lon or 0, lat or 0
        )
    )

    # Insere na tabela human_impacts
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

    # Insere na tabela information_sources
    src_date = parse_date(row.get("sourcedate"))
    cur.execute(
        """
        INSERT INTO information_sources (
            disaster_id, source_name, source_date, source_type, page
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (
            disaster_id,
            safe_str(row.get("source")),
            src_date,
            safe_str(row.get("sourcetype")),
            safe_str(row.get("page"))
        )
    )

# -----------------------------
# Commit & fechar
# -----------------------------
conn.commit()
cur.close()
conn.close()
print("Importação concluída e ligação encerrada.")
