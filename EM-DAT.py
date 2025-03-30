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

# Função auxiliar para converter valores numéricos dentro do limite de INTEGER
def safe_int(x):
    try:
        x_int = int(float(x))
        return x_int if x_int < 2147483647 else 2147483647
    except:
        return 0

# Função auxiliar para truncar strings a um comprimento máximo (ex.: 100 chars)
def safe_str(x, max_length=100):
    try:
        s = str(x)
        return s if len(s) <= max_length else s[:max_length]
    except:
        return ""

# Função para extrair o nome do distrito a partir de uma string que deveria ser JSON
# mas pode estar truncada ou conter NaN
def extract_district_name(admin_units_value):
    # 1. Se for NaN ou None, devolve "Desconhecido"
    if pd.isna(admin_units_value) or not isinstance(admin_units_value, str):
        return "Desconhecido"
    
    raw_text = admin_units_value.strip()
    if not raw_text:
        return "Desconhecido"

    # 2. Tentar parse JSON completo
    try:
        admin_list = json.loads(raw_text)  # converte para lista/dict
        # Procurar adm1_name ou adm2_name no primeiro objeto
        if admin_list:
            for item in admin_list:
                if "adm1_name" in item:
                    return item["adm1_name"]
                elif "adm2_name" in item:
                    return item["adm2_name"]
        return "Desconhecido"
    except:
        # 3. Fallback: usar regex para procurar "adm1_name":"..."
        pattern1 = re.compile(r'"adm1_name":"([^"]+)"')
        match1 = pattern1.search(raw_text)
        if match1:
            return match1.group(1)

        # Se não achou adm1_name, tenta adm2_name
        pattern2 = re.compile(r'"adm2_name":"([^"]+)"')
        match2 = pattern2.search(raw_text)
        if match2:
            return match2.group(1)

        # Se não encontrar nada, devolve "Desconhecido"
        return "Desconhecido"

# ================================
# Parte 1: Download do ficheiro Excel
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
    # 1. Login no EM-DAT
    driver.get('https://public.emdat.be/login')
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, 'username'))
    )
    username_input = driver.find_element(By.ID, 'username')
    password_input = driver.find_element(By.ID, 'password')
    
    # Insere as credenciais
    username_input.send_keys('joselffernandes@gmail.com')
    password_input.send_keys('Middle_1985')
    password_input.send_keys(Keys.RETURN)
    
    # Aguarda até que a página pós-login carregue
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Access Data')]"))
    )
    
    # 2. Clica no botão "Go" para aceder à página de dados
    go_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//a[@href='/data']/button"))
    )
    go_button.click()
    
    # Aguarda que a página de dados carregue
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, 'body'))
    )
    
    # 3. Selecionar a classificação "Natural"
    classification_dropdown = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "(//div[contains(@class, 'ant-select-selector')])[1]"))
    )
    classification_dropdown.click()
    
    natural_option = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//span[@class='ant-select-tree-title' and normalize-space(text())='Natural']"))
    )
    natural_option.click()
    
    # 4. Selecionar o país "Europe" no campo "countries"
    countries_dropdown = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "(//div[contains(@class, 'ant-select-selector')])[2]"))
    )
    countries_dropdown.click()
    
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'ant-select-tree')]"))
    )
    
    europe_option = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//span[@class='ant-select-tree-title' and normalize-space(text())='Europe']"))
    )
    europe_option.click()
    
    # 5. Clicar no botão de download (ícone com aria-label="download")
    download_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//span[@aria-label='download']"))
    )
    download_button.click()
    
    print("Login efetuado, classificações selecionadas e download iniciado com sucesso!")
    
    # Aguarda alguns segundos para concluir o download
    time.sleep(10)
    print("O ficheiro foi descarregado na pasta:", download_dir)
    
except Exception as e:
    print("Erro durante o download:", e)
    
finally:
    time.sleep(5)
    driver.quit()

# ================================
# Parte 2: Importar os dados do ficheiro Excel para o PostgreSQL
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
        # Filtrar para apenas os registros onde:
        # - A coluna 'Country' corresponde a 'Portugal'
        # - A coluna 'Disaster Type' corresponde a 'Flood'
        df = df[(df["Country"].str.lower() == "portugal") & (df["Disaster Type"].str.lower() == "flood")]
        
        if df.empty:
            print("Nenhum registro encontrado para os critérios: Portugal e Flood.")
        else:
            try:
                # Ligação à base de dados PostgreSQL
                conn = psycopg2.connect(
                    dbname="simpred",               # Nome da base
                    user="joseferreirafernandes",    # Utilizador
                    password="sua_senha",            # Substituir pela tua senha
                    host="localhost",
                    port="5432"
                )
                cur = conn.cursor()
                
                print("A inserir dados nas tabelas...")
                
                for i, row in df.iterrows():
                    # Converter Start Year, Start Month, Start Day
                    year_val = safe_int(row["Start Year"])
                    month_val = safe_int(row["Start Month"])
                    day_val = safe_int(row["Start Day"])
                    
                    # Ajustar valores 0 -> 1
                    if month_val == 0:
                        month_val = 1
                    if day_val == 0:
                        day_val = 1
                    
                    # Construir data
                    try:
                        event_dt = pd.to_datetime(f"{year_val}-{month_val:02d}-{day_val:02d}")
                        event_date = event_dt.date()
                    except Exception as date_error:
                        event_date = None
                    
                    # Inserir na tabela disasters
                    cur.execute("""
                        INSERT INTO disasters (type, subtype, date, year, month, day, hour)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """, (
                        safe_str(row["Disaster Type"], 100),
                        safe_str(row["Disaster Subtype"], 100),
                        event_date,
                        year_val,
                        month_val,
                        day_val,
                        "00:00-00:00"
                    ))
                    disaster_id = cur.fetchone()[0]

                    # Extrair apenas o nome do distrito (adm1_name/adm2_name) a partir de Admin Units
                    district_name = extract_district_name(row["Admin Units"])

                    # Inserir na tabela location
                    cur.execute("""
                        INSERT INTO location (id, latitude, longitude, georef_class, district, municipality, parish, DICOFREG, geom)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                    """, (
                        disaster_id,
                        float(row["Latitude"]),
                        float(row["Longitude"]),
                        None,
                        safe_str(district_name, 100),
                        safe_str(row["Region"], 100),
                        safe_str(row["Location"], 100),
                        None,
                        float(row["Longitude"]),
                        float(row["Latitude"])
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

                    # Inserir na tabela information_sources
                    try:
                        entry_dt = pd.to_datetime(row["Entry Date"])
                        source_date = entry_dt.date()
                    except:
                        source_date = None

                    cur.execute("""
                        INSERT INTO information_sources (disaster_id, source_name, source_date, source_type, page)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        disaster_id,
                        safe_str(row["Historic"], 100),
                        source_date,
                        "EM-DAT",  # Valor fixo para indicar a origem dos dados
                        "-"
                    ))
                
                conn.commit()
                cur.close()
                conn.close()
                print("Tabelas populadas com sucesso com dados do EM-DAT.")
            
            except Exception as db_error:
                print("Erro ao importar dados para o PostgreSQL:", db_error)
else:
    print("Nenhum ficheiro Excel encontrado na pasta:", download_dir)
