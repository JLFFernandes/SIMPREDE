# EM-DAT_final.py (com divisão proporcional dos impactos humanos sem perdas)

import os
import time
import json
import re
import requests
import pandas as pd
import geopandas as gpd
import psycopg2
from shapely.geometry import Point
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

# Diretório de download
script_dir = os.path.dirname(os.path.abspath(__file__))
download_dir = os.path.join(script_dir, "temp_downloads")
os.makedirs(download_dir, exist_ok=True)
if not os.access(download_dir, os.W_OK):
    raise PermissionError(f"Sem permissões de escrita em {download_dir}")

# Conexão à base de dados Supabase
DB_PARAMS = {
    "host": "aws-0-eu-west-3.pooler.supabase.com",
    "port": "6543",
    "dbname": "postgres",
    "user": "postgres.kyrfsylobmsdjlrrpful",
    "password": "HXU3tLVVXRa1jtjo",
    "sslmode": "require"
}

CENTROIDES_SHP = "/usr/local/airflow/include/centroides/centroide_concel/shape_conc_centroide.shp"
gdf_concelhos = gpd.read_file(CENTROIDES_SHP).to_crs(epsg=4326)

MANUAL_REGION_MAP = {
    "Douro": ("Vila Real", "Vila Real"),
    "Baixo Vouga": ("Aveiro", "Aveiro"),
    "Grande Lisboa": ("Lisboa", "Lisboa"),
    "Cova da Beira": ("Castelo Branco", "Covilhã"),
    "Médio Tejo": ("Santarém", "Tomar"),
    "Dão-Lafões": ("Viseu", "Viseu"),
    "Alto Trás-os-Montes": ("Bragança", "Bragança"),
    "Tâmega": ("Porto", "Amarante"),
    "Lezíria do Tejo": ("Santarém", "Santarém"),
    "Oeste": ("Leiria", "Caldas da Rainha"),
    "Baixo Mondego": ("Coimbra", "Coimbra"),
    "Beira Interior Sul": ("Castelo Branco", "Idanha-a-Nova"),
    "Beira Interior Norte": ("Guarda", "Guarda"),
    "Cávado": ("Braga", "Braga"),
    "Setubal": ("Setúbal", "Setúbal"),
    "Madeira Island": ("Madeira", "Funchal"),
    "Faro province": ("Faro", "Faro"),
    "Grande Porto": ("Porto", "Porto"),
    "Península de Setúbal": ("Setúbal", "Setúbal"),
    "North": ("Braga", "Braga"),
    "Lisbon": ("Lisboa", "Lisboa"),
    "Seia district (Guarda province)": ("Guarda", "Seia"),
    "Mesao Frio district (Vila Real province)": ("Vila Real", "Mesão Frio"),
    "Agueda": ("Aveiro", "Águeda"),
    "Anadia": ("Aveiro", "Anadia"),
    "Mealhada": ("Aveiro", "Mealhada"),
    "Oliveira do Bairro": ("Aveiro", "Oliveira do Bairro"),
    "Sacavem cities": ("Lisboa", "Loures"),
    "Setubal city": ("Setúbal", "Setúbal"),
    "Loures": ("Lisboa", "Loures"),
    "Funchal": ("Madeira", "Funchal"),
    "Ilha Da Madeira": ("Madeira", "Funchal"),
    "Mesao Frio": ("Vila Real", "Mesão Frio"),
    "Sacavém": ("Lisboa", "Loures"),
    "Ilha da Madeira": ("Madeira", "Funchal"),
    "Mesão Frio": ("Vila Real", "Mesão Frio"),
    "Sacavem": ("Lisboa", "Loures")
}

def safe_int(x):
    try:
        i = int(float(x))
        return i if i < 2147483647 else 2147483647
    except:
        return 0

def safe_str(x, max_length=100):
    try:
        s = str(x)
        return s if len(s) <= max_length else s[:max_length]
    except:
        return ""

def distribuir_valores(total, n):
    base = total // n
    resto = total % n
    return [base + 1 if i < resto else base for i in range(n)]

def get_centroid_coords(district, municipality):
    try:
        match = gdf_concelhos[
            (gdf_concelhos["NAME_1"].str.lower() == district.lower()) &
            (gdf_concelhos["NAME_2"].str.lower() == municipality.lower())
        ]
        if not match.empty:
            geom = match.iloc[0].geometry
            return geom.y, geom.x
    except:
        pass
    return None, None

def expand_location_rows(df):
    expanded = []
    for _, row in df.iterrows():
        loc_text = re.sub(r'\([^)]*\)', '', str(row['Location']))
        loc_text = re.sub(r'\bdistricts?\b|\bprovinces?\b', '', loc_text)
        loc_text = loc_text.replace('Ilha Da Madeira', 'Funchal')
        loc_text = re.sub(r'\bLoure\b', 'Loures', loc_text)
        parts = [p.strip() for p in re.split(r',\s*|\band\b', loc_text) if p.strip()]
        n = len(parts) or 1
        deaths = distribuir_valores(safe_int(row["Total Deaths"]), n)
        injured = distribuir_valores(safe_int(row["No. Injured"]), n)
        homeless = distribuir_valores(safe_int(row["No. Homeless"]), n)
        for i, loc in enumerate(parts):
            new_row = row.copy()
            new_row["Location"] = loc
            new_row["split_count"] = n
            new_row["split_deaths"] = deaths[i]
            new_row["split_injured"] = injured[i]
            new_row["split_homeless"] = homeless[i]
            expanded.append(new_row)
    return pd.DataFrame(expanded)

def atribuir_distrito_municipio(row):
    loc = row['Location']
    if loc in MANUAL_REGION_MAP:
        row['district'], row['municipality'] = MANUAL_REGION_MAP[loc]
        row['georef_class'] = 2
    else:
        match = gdf_concelhos[gdf_concelhos['NAME_2'].str.lower() == loc.lower()]
        if not match.empty:
            row['municipality'] = match.iloc[0]['NAME_2']
            row['district'] = match.iloc[0]['NAME_1']
            row['georef_class'] = 3
        else:
            row['district'] = "Desconhecido"
            row['municipality'] = loc
            row['georef_class'] = 0
    return row

CHROMEDRIVER_PATH = "/usr/bin/chromedriver"
def tentar_download_com_selenium():
    print("A aceder ao site EM-DAT via Selenium...")
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "profile.default_content_settings.popups": 0
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    service = Service(executable_path=CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get('https://public.emdat.be/login')
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, 'username')))
        driver.find_element(By.ID, 'username').send_keys('joselffernandes@gmail.com')
        driver.find_element(By.ID, 'password').send_keys('Middle_1985', Keys.RETURN)
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//a[@href='/data']/button"))).click()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "(//div[contains(@class,'ant-select-selector')])[1]"))).click()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[normalize-space(text())='Natural']"))).click()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "(//div[contains(@class,'ant-select-selector')])[2]"))).click()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[@class='ant-select-tree-title' and normalize-space(text())='Europe']"))).click()
        hist_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, 'include_hist')))
        if hist_btn.get_attribute('aria-checked') != 'true':
            hist_btn.click()
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[@aria-label='download']"))).click()
        timeout = time.time() + 120
        while time.time() < timeout:
            files = [f for f in os.listdir(download_dir) if f.lower().endswith(('.xlsx', '.xls'))]
            if files:
                return os.path.join(download_dir, files[0])
            time.sleep(2)
        raise TimeoutError("Download não concluído dentro do tempo limite.")
    finally:
        driver.quit()

# Execução principal
try:
    excel_path = tentar_download_com_selenium()
except Exception as err:
    print("Erro com Selenium:", err)
    raise

print("A ler ficheiro:", excel_path)
df = pd.read_excel(excel_path)
df = df[(df["Country"].str.lower() == "portugal") & (df["Disaster Type"].str.lower() == "flood")]
if df.empty:
    print("Sem registos relevantes.")
    exit(0)

df = expand_location_rows(df)
df = df.apply(atribuir_distrito_municipio, axis=1)

conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()
print("Ligado à base de dados. A inserir...")

for _, row in df.iterrows():
    y, m, d = safe_int(row["Start Year"]), safe_int(row["Start Month"]) or 1, safe_int(row["Start Day"]) or 1
    try:
        ev_date = pd.to_datetime(f"{y}-{m:02d}-{d:02d}").date()
    except:
        ev_date = None

    cur.execute("""
        INSERT INTO disasters (type, subtype, date, year, month, day, hour)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        safe_str(row["Disaster Type"]), safe_str(row.get("Disaster Subtype") or "General flood"),
        ev_date, y, m, d, "00:00-00:00"
    ))
    did = cur.fetchone()[0]

    lat, lon = get_centroid_coords(row['district'], row['municipality'])
    georef_class = row['georef_class']
    if lat is None or lon is None:
        lat, lon = 39.5, -8.0
        georef_class = 0

    cur.execute("""
        INSERT INTO location (id, latitude, longitude, georef_class, district, municipality, parish, dicofreg, geom)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326))
    """, (did, lat, lon, georef_class, safe_str(row['district']), safe_str(row['municipality']), row['Location'], None, lon, lat))

    cur.execute("""
        INSERT INTO human_impacts (id, fatalities, injured, evacuated, displaced, missing)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        did,
        row["split_deaths"],
        row["split_injured"],
        0,
        row["split_homeless"],
        0
    ))

    try:
        sd = pd.to_datetime(row["Entry Date"]).date()
    except:
        sd = None
    stype = "www.emdat.be"
    scrat = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""
        INSERT INTO information_sources (disaster_id, source_name, source_date, source_type, page)
        VALUES (%s, %s, %s, %s, %s)
    """, (did, f"{stype} (extraído em {scrat})", sd, stype, "-"))

conn.commit()
cur.close()
conn.close()
print("Importação concluída com sucesso!")
