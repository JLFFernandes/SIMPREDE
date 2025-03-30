import os
import time
import json
import re
import pandas as pd
import psycopg2
from datetime import datetime, date

# -----------------------------
# Funções auxiliares
# -----------------------------
def safe_int(x):
    """Converte o valor para inteiro, respeitando o limite máximo do INTEGER.
    Retorna 0 se não for possível converter."""
    try:
        x_int = int(float(x))
        return x_int if x_int < 2147483647 else 2147483647
    except:
        return 0

def safe_str(x, max_length=100):
    """Trunca uma string para um comprimento máximo."""
    try:
        s = str(x)
        return s if len(s) <= max_length else s[:max_length]
    except:
        return ""

def parse_geom(geom_str):
    """
    Extrai latitude e longitude de uma string no formato "POINT(lon lat)".
    Retorna (lat, lon) ou (None, None) se não conseguir.
    """
    try:
        if pd.isna(geom_str) or not isinstance(geom_str, str):
            return None, None
        geom_str = geom_str.strip()
        if geom_str.startswith("POINT(") and geom_str.endswith(")"):
            coords = geom_str[6:-1].split()
            if len(coords) == 2:
                lon = float(coords[0])
                lat = float(coords[1])
                return lat, lon
    except:
        return None, None
    return None, None

def parse_date(date_value):
    """
    Converte o valor da data para um objeto date.
    Se o valor for string e contiver '/', assume formato dia/mês/ano (dayfirst=True);
    caso contrário, usa a conversão padrão.
    """
    if pd.isnull(date_value):
        return None
    try:
        if isinstance(date_value, str):
            if "/" in date_value:
                return pd.to_datetime(date_value, dayfirst=True).date()
            else:
                return pd.to_datetime(date_value).date()
        else:
            return pd.to_datetime(date_value).date()
    except:
        return None

# -----------------------------
# Leitura do ficheiro Excel
# -----------------------------
excel_file_path = "/Users/joseferreirafernandes/Desktop/5 ano/Projeto/fase 2/doc/pts_disaster_export.xlsx"

try:
    df = pd.read_excel(excel_file_path)
    print("Ficheiro Excel lido com sucesso!")
    print("Colunas encontradas:", df.columns.tolist())
except Exception as e:
    print("Erro ao ler o ficheiro Excel:", e)
    df = None

# -----------------------------
# Inserção dos dados na base de dados PostgreSQL
# -----------------------------
if df is not None:
    try:
        # Estabelece a ligação à base de dados
        conn = psycopg2.connect(
            dbname="simpred",                # Nome da base de dados
            user="joseferreirafernandes",     # Nome do utilizador
            password="sua_senha",             # Substituir pela senha correta
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        print("Ligação à base de dados estabelecida com sucesso.")
        print("A inserir dados nas tabelas...")

        for i, row in df.iterrows():
            # --- Tratamento da data do evento ---
            event_date = parse_date(row.get("date"))
            if event_date is None:
                # Se a coluna "date" não estiver disponível, constrói a data a partir de year, month e day
                year_val = safe_int(row.get("year"))
                month_val = safe_int(row.get("month"))
                day_val = safe_int(row.get("day"))
                if year_val == 0:
                    event_date = pd.to_datetime("1900-01-01").date()
                else:
                    if month_val == 0:
                        month_val = 1
                    if day_val == 0:
                        day_val = 1
                    try:
                        event_date = pd.to_datetime(f"{year_val}-{month_val:02d}-{day_val:02d}").date()
                    except Exception as date_error:
                        event_date = pd.to_datetime("1900-01-01").date()

            # --- Inserção na tabela disasters ---
            cur.execute("""
                INSERT INTO disasters (type, subtype, date, year, month, day, hour)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (
                safe_str(row.get("type")),
                safe_str(row.get("subtype")),
                event_date,
                safe_int(row.get("year")),
                safe_int(row.get("month")),
                safe_int(row.get("day")),
                safe_str(row.get("hour"))
            ))
            disaster_id = cur.fetchone()[0]

            # --- Inserção na tabela location ---
            lat, lon = parse_geom(row.get("geom"))
            # Trata o campo "georef": se não for um valor numérico válido, define um valor padrão (ex.: 1)
            georef_class = safe_int(row.get("georef"))
            if georef_class == 0:
                georef_class = 1

            cur.execute("""
                INSERT INTO location (id, latitude, longitude, georef_class, district, municipality, parish, DICOFREG, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            """, (
                disaster_id,
                lat if lat is not None else 0,
                lon if lon is not None else 0,
                georef_class,
                safe_str(row.get("district")),
                safe_str(row.get("municipali")),
                safe_str(row.get("parish")),
                safe_str(row.get("dicofreg")),
                lon if lon is not None else 0,
                lat if lat is not None else 0
            ))

            # --- Inserção na tabela human_impacts ---
            cur.execute("""
                INSERT INTO human_impacts (id, fatalities, injured, evacuated, displaced, missing)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                disaster_id,
                safe_int(row.get("fatalities")),
                safe_int(row.get("injured")),
                safe_int(row.get("evacuated")),
                safe_int(row.get("displaced")),
                safe_int(row.get("missing"))
            ))

            # --- Inserção na tabela information_sources ---
            source_date = parse_date(row.get("sourcedate"))
            cur.execute("""
                INSERT INTO information_sources (disaster_id, source_name, source_date, source_type, page)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                disaster_id,
                safe_str(row.get("source")),
                source_date,
                safe_str(row.get("sourcetype")),
                safe_str(row.get("page"))
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        print("Tabelas populadas com sucesso com os dados do ficheiro Excel.")

    except Exception as db_error:
        print("Erro ao importar dados para o PostgreSQL:", db_error)
else:
    print("Não foi possível processar o ficheiro Excel.")
