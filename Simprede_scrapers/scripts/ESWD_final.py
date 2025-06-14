import psycopg2
import pandas as pd
from datetime import datetime

# === CONFIGURAÇÃO DE LIGAÇÃO AO SUPABASE ===
DB_HOST = "aws-0-eu-west-3.pooler.supabase.com"
DB_NAME = 'postgres'
DB_USER = 'postgres.kyrfsylobmsdjlrrpful'
DB_PASSWORD = 'HXU3tLVVXRa1jtjo'
DB_PORT = 5432
CSV_PATH = '/usr/local/airflow/include/eswd_database.csv'

# === CARREGAR CSV E PREPARAR DADOS ===
df = pd.read_csv(CSV_PATH)
df.columns = df.columns.str.upper().str.strip()
df = df[df['TIME_EVENT'].notnull()]
df['TIME_EVENT'] = pd.to_datetime(df['TIME_EVENT'], errors='coerce')
df['DATE'] = df['TIME_EVENT'].dt.date
df['HOUR'] = df['TIME_EVENT'].dt.strftime('%H:%M')

# === LIGAÇÃO À BASE DE DADOS ===
conn = psycopg2.connect(
    host=DB_HOST, dbname=DB_NAME, user=DB_USER,
    password=DB_PASSWORD, port=DB_PORT
)
cur = conn.cursor()
print("Ligação estabelecida ao Supabase")

print(f"Linhas lidas do CSV ESWD: {len(df)}")

for _, row in df.iterrows():
    try:
        dt = row['TIME_EVENT']
        year, month, day = dt.year, dt.month, dt.day
        hour = dt.strftime("%H:%M")

        # === INSERIR DESASTRE ===
        cur.execute("""
            INSERT INTO eswd_testing.disasters (type, subtype, date, year, month, day, hour)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (
            row.get('TYPE_EVENT') or 'unknown',
            'Heavy Rain',
            dt.date(), year, month, day, hour
        ))
        disaster_id = cur.fetchone()[0]

        # === PARISH ===
        parish = str(row.get('DETAILED_LOCATION')).strip() if pd.notnull(row.get('DETAILED_LOCATION')) else None
        if not parish or parish.lower() in ['nan', 'none', '']:
            parish = str(row.get('PLACE')).strip() if pd.notnull(row.get('PLACE')) else None

        # === LOCALIZAÇÃO ===
        lat = float(row['LATITUDE']) if 'LATITUDE' in row and pd.notnull(row['LATITUDE']) else None
        lon = float(row['LONGITUDE']) if 'LONGITUDE' in row and pd.notnull(row['LONGITUDE']) else None

        if lat is not None and lon is not None:
            cur.execute("""
                INSERT INTO eswd_testing.location (id, latitude, longitude, georef_class,
                                             district, municipality, parish, dicofreg, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326));
            """, (
                disaster_id, lat, lon, 1,
                str(row.get('STATE')).strip() if pd.notnull(row.get('STATE')) else None,
                str(row.get('PLACE')).strip() if pd.notnull(row.get('PLACE')) else None,
                parish,
                None,
                lon, lat
            ))

        # === IMPACTOS HUMANOS ===
        cur.execute("""
            INSERT INTO eswd_testing.human_impacts
            (id, fatalities, injured, evacuated, displaced, missing)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            disaster_id,
            int(row['FATALITIES']) if 'FATALITIES' in row and pd.notnull(row['FATALITIES']) else None,
            int(row['INJURED']) if 'INJURED' in row and pd.notnull(row['INJURED']) else None,
            int(row['EVACUATED']) if 'EVACUATED' in row and pd.notnull(row['EVACUATED']) else None,
            int(row['DISPLACED']) if 'DISPLACED' in row and pd.notnull(row['DISPLACED']) else None,
            int(row['MISSING']) if 'MISSING' in row and pd.notnull(row['MISSING']) else None
        ))

        # === FONTE DE INFORMAÇÃO ===
        cur.execute("""
            INSERT INTO eswd_testing.information_sources
            (disaster_id, source_name, source_date, source_type, page)
            VALUES (%s, %s, %s, %s, %s);
        """, (
            disaster_id,
            str(row.get('INFO_SOURCE')).strip() if pd.notnull(row.get('INFO_SOURCE')) else None,
            dt.date(),
            'online',
            str(row.get('EXT_URL')).strip() if pd.notnull(row.get('EXT_URL')) else None
        ))

        conn.commit()

    except Exception as e:
        print(f"Erro na linha {row.get('ID', 'desconhecido')}: {e}")
        conn.rollback()

cur.close()
conn.close()
print("Importação concluída com sucesso.")
