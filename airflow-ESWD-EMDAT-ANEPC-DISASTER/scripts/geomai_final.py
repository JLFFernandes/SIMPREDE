import requests
import time
import pandas as pd
import sys
import psycopg2
from datetime import date
import os

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

BASE_URL = (
    "https://prociv-agserver.geomai.mai.gov.pt/arcgis/rest/services/"
    "Ocorrencias_Base/FeatureServer/0/query"
)
HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "pt-PT,pt;q=0.9,en-US;q=0.8",
    "Origin": "https://prociv-portal.geomai.mai.gov.pt",
    "Referer": "https://prociv-portal.geomai.mai.gov.pt/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
}
PARAMS = {
    "f": "json",
    "where": "Natureza='Cheia'",
    "outFields": "*",
    "returnGeometry": "false",
    "resultOffset": 0,
    "resultRecordCount": 200,
}
EXP_URL = (
    "https://prociv-portal.geomai.mai.gov.pt/arcgis/apps/"
    "experiencebuilder/experience/?id=29e83f11f7a34339b35364e483e3846f&page=Oc"
)

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
        return s if len(s) <= max_len else s[:max_len]
    except:
        return ""

def parse_geom(lat, lon):
    try:
        if pd.isna(lat) or pd.isna(lon):
            return None, None
        return float(lat), float(lon)
    except:
        return None, None

def fetch_records():
    all_attrs, offset = [], 0
    while True:
        PARAMS["resultOffset"] = offset
        try:
            resp = requests.get(BASE_URL, headers=HEADERS, params=PARAMS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Erro ao obter offset={offset}: {e}", file=sys.stderr)
            time.sleep(5)
            continue

        feats = resp.json().get("features", [])
        if not feats:
            break
        all_attrs.extend(f["attributes"] for f in feats)
        print(f"Offset {offset}: obtidos {len(feats)} (total {len(all_attrs)})")
        offset += PARAMS["resultRecordCount"]
        time.sleep(1)
    return all_attrs

def build_dataframe(records):
    df = pd.DataFrame(records)

    dt_iso = pd.to_datetime(df["DataInicioOcorrencia"], errors="coerce")
    dt_ms  = pd.to_datetime(df["DataOcorrencia"], unit="ms", errors="coerce")
    full_dt = dt_iso.fillna(dt_ms)
    date_col = full_dt.dt.normalize()
    src_dt = pd.to_datetime(df["DataDosDados"], unit="ms", errors="coerce").dt.normalize()
    geom = "POINT(" + df["Longitude"].astype(str) + " " + df["Latitude"].astype(str) + ")"

    return pd.DataFrame({
        "type":        df["Natureza"],
        "date":        date_col,
        "year":        date_col.dt.year,
        "month":       date_col.dt.month,
        "day":         date_col.dt.day,
        "hour":        full_dt.dt.strftime("%H:%M"),
        "latitude":    df["Latitude"],
        "longitude":   df["Longitude"],
        "source":      EXP_URL,
        "sourcedate":  src_dt,
        "sourcetype":  "ArcGISFeatureServer",
        "page":        0,
        "fatalities":  0, "injured":    0,
        "evacuated":   0, "displaced":  0,
        "missing":     0,
        "district":    df["Concelho"],
        "municipali":  df["Concelho"],
        "parish":      df["Freguesia"],
        "dicofreg":    df["DICOFRE"],
        "geom":        geom
    })

def insert_into_db(df):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    print("Ligação estabelecida ao Supabase")

    for _, row in df.iterrows():
        ev_date = row.get("date").date() if pd.notnull(row.get("date")) else date(1900, 1, 1)
        lat, lon = parse_geom(row.get("latitude"), row.get("longitude"))

        # Verificação de duplicado com base em conteúdo (não em ID)
        cur.execute(
            """
            SELECT id FROM disasters
            WHERE type = %s AND date = %s AND hour = %s
            """,
            (
                safe_str(row.get("type")),
                ev_date,
                safe_str(row.get("hour")),
            )
        )
        possible_matches = cur.fetchall()
        duplicado = False

        for match in possible_matches:
            cur.execute(
                """
                SELECT 1 FROM location
                WHERE id = %s
                  AND municipality = %s
                  AND parish = %s
                  AND ABS(latitude - %s) < 0.00001
                  AND ABS(longitude - %s) < 0.00001
                """,
                (
                    match[0],
                    safe_str(row.get("municipali")),
                    safe_str(row.get("parish")),
                    lat or 0,
                    lon or 0
                )
            )
            if cur.fetchone():
                print(f"Registo duplicado ignorado: {ev_date} {row.get('municipali')} {row.get('parish')}")
                duplicado = True
                break

        if duplicado:
            continue

        # Inserção nas tabelas
        cur.execute(
            """
            INSERT INTO disasters (
                type, subtype, date, year, month, day, hour
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                safe_str(row.get("type")),
                None,
                ev_date,
                safe_int(row.get("year")),
                safe_int(row.get("month")),
                safe_int(row.get("day")),
                safe_str(row.get("hour"))
            )
        )
        disaster_id = cur.fetchone()[0]

        georef_cls = 1
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

        src_date = row.get("sourcedate").date() if pd.notnull(row.get("sourcedate")) else None
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

    conn.commit()
    cur.close()
    conn.close()
    print("Importação concluída e ligação encerrada.")

def main():
    print("Iniciando download de ocorrências de Cheia...")
    new_recs = fetch_records()
    print(f"{len(new_recs)} registos encontrados")
    df_new = build_dataframe(new_recs)
    insert_into_db(df_new)

if __name__ == "__main__":
    main()
