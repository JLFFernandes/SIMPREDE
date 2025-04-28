import os
import time
import json
import re
import pandas as pd
import psycopg2
from datetime import datetime, date
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

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
    try:
        x_int = int(float(x))
        return x_int if x_int < 2147483647 else 2147483647
    except:
        return 0

def safe_str(x, max_length=100):
    try:
        s = str(x)
        return s if len(s) <= max_length else s[:max_length]
    except:
        return ""

def extract_district_name(admin_units_value):
    if pd.isna(admin_units_value) or not isinstance(admin_units_value, str):
        return "Desconhecido"
    
    raw_text = admin_units_value.strip()
    if not raw_text:
        return "Desconhecido"

    try:
        admin_list = json.loads(raw_text)
        if admin_list:
            for item in admin_list:
                if "adm1_name" in item:
                    return item["adm1_name"]
                elif "adm2_name" in item:
                    return item["adm2_name"]
        return "Desconhecido"
    except:
        pattern1 = re.compile(r'"adm1_name":"([^"]+)"')
        match1 = pattern1.search(raw_text)
        if match1:
            return match1.group(1)

        pattern2 = re.compile(r'"adm2_name":"([^"]+)"')
        match2 = pattern2.search(raw_text)
        if match2:
            return match2.group(1)

        return "Desconhecido"

# ================================
# Parte 1: Download do ficheiro Excel via Selenium
# ================================

download_dir = os.path.join(os.getcwd(), "temp_downloads")
os.makedirs(download_dir, exist_ok=True)

options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True
}
options.add_experimental_option("prefs", prefs)

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

try:
    driver.get('https://public.emdat.be/login')
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'username')))
    username_input = driver.find_element(By.ID, 'username')
    password_input = driver.find_element(By.ID, 'password')
    username_input.send_keys('joselffernandes@gmail.com')
    password_input.send_keys('Middle_1985')
    password_input.send_keys(Keys.RETURN)

    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Access Data')]")))
    go_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@href='/data']/button")))
    go_button.click()

    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    classification_dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "(//div[contains(@class, 'ant-select-selector')])[1]")))
    classification_dropdown.click()
    natural_option = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[@class='ant-select-tree-title' and normalize-space(text())='Natural']")))
    natural_option.click()

    countries_dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "(//div[contains(@class, 'ant-select-selector')])[2]")))
    countries_dropdown.click()
    WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'ant-select-tree')]")))
    europe_option = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[@class='ant-select-tree-title' and normalize-space(text())='Europe']")))
    europe_option.click()

    download_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[@aria-label='download']")))
    download_button.click()
    
    print("Download iniciado com sucesso!")
    time.sleep(10)
    
except Exception as e:
    print("Erro durante o download:", e)
    
finally:
    time.sleep(5)
    driver.quit()

# ================================
# Parte 2: Importação dos dados para Supabase
# ================================

excel_files = [f for f in os.listdir(download_dir) if f.lower().endswith(('.xlsx', '.xls'))]

if excel_files:
    excel_file_path = os.path.join(download_dir, excel_files[0])
    print("Ficheiro Excel encontrado:", excel_file_path)
    
    try:
        df = pd.read_excel(excel_file_path)
        print("Colunas do Excel:", df.columns.tolist())
    except Exception as e:
        print("Erro ao ler o ficheiro Excel:", e)
        df = None

    if df is not None:
        df = df[(df["Country"].str.lower() == "portugal") & (df["Disaster Type"].str.lower() == "flood")]
        
        if df.empty:
            print("Nenhum registro encontrado para os critérios: Portugal e Flood.")
        else:
            try:
                conn = psycopg2.connect(**DB_PARAMS)
                cur = conn.cursor()
                print("Ligação estabelecida ao Supabase. A inserir dados...")

                for _, row in df.iterrows():
                    year_val = safe_int(row["Start Year"])
                    month_val = safe_int(row["Start Month"]) or 1
                    day_val = safe_int(row["Start Day"]) or 1
                    
                    try:
                        event_dt = pd.to_datetime(f"{year_val}-{month_val:02d}-{day_val:02d}")
                        event_date = event_dt.date()
                    except:
                        event_date = None
                    
                    # Inserir na tabela disasters
                    cur.execute("""
                        INSERT INTO disasters (type, subtype, date, year, month, day, hour)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """, (
                        safe_str(row["Disaster Type"]),
                        safe_str(row["Disaster Subtype"]),
                        event_date,
                        year_val,
                        month_val,
                        day_val,
                        "00:00-00:00"
                    ))
                    disaster_id = cur.fetchone()[0]

                    district_name = extract_district_name(row["Admin Units"])

                    # Corrigir valores NaN de latitude/longitude
                    lat = float(row["Latitude"]) if pd.notna(row["Latitude"]) else 0.0
                    lon = float(row["Longitude"]) if pd.notna(row["Longitude"]) else 0.0

                    # Inserir na tabela location com georef_class fixo
                    cur.execute("""
                        INSERT INTO location (id, latitude, longitude, georef_class, district, municipality, parish, dicofreg, geom)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                    """, (
                        disaster_id,
                        lat,
                        lon,
                        1,  # fixo!
                        safe_str(district_name),
                        safe_str(row["Region"]),
                        safe_str(row["Location"]),
                        None,
                        lon,
                        lat
                    ))

                    # Inserir na tabela human_impacts
                    cur.execute("""
                        INSERT INTO human_impacts (id, fatalities, injured, evacuated, displaced, missing)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        disaster_id,
                        safe_int(row["Total Deaths"]),
                        safe_int(row["No. Injured"]),
                        0,
                        safe_int(row["No. Homeless"]),
                        0
                    ))

                    try:
                        entry_dt = pd.to_datetime(row["Entry Date"])
                        source_date = entry_dt.date()
                    except:
                        source_date = None

                    # Inserir na tabela information_sources
                    cur.execute("""
                        INSERT INTO information_sources (disaster_id, source_name, source_date, source_type, page)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        disaster_id,
                        safe_str(row["Historic"]),
                        source_date,
                        "www.emdat.be",
                        "-"
                    ))
                
                conn.commit()
                cur.close()
                conn.close()
                print("Tabelas populadas com sucesso com dados do EM-DAT para Supabase.")
            
            except Exception as db_error:
                print("Erro ao importar dados para o Supabase:", db_error)
else:
    print("Nenhum ficheiro Excel encontrado na pasta:", download_dir)
