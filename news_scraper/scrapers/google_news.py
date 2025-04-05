import feedparser
import requests
import time
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from utils.helpers import (
    extract_article_text, extract_victim_counts,
    detect_disaster_type, detect_municipality, load_keywords,
    load_localidades, is_potentially_disaster_related,
    parse_event_date, guardar_csv, guardar_csv_incremental,
    carregar_municipios_distritos, carregar_paroquias_com_municipios,
    load_freguesias_codigos, carregar_dicofreg, normalize
)
import sys
import os
import signal
import csv
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from test_scraper import get_real_url_with_newspaper, extract_article_content
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==============================
# CONFIG
# ==============================
GOOGLE_NEWS_TEMPLATE = "https://news.google.com/rss/search?q={query}+{municipio}+Portugal&hl=pt-PT&gl=MZ&ceid=MZ:pt-150"
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")
LOCALIDADES = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")


OUTPUT_CSV = "data/artigos_google_municipios_pt.csv"
HEADERS = {"User-Agent": "MunicipioNewsBot/1.0"}


# ==============================
# FUNCOES AUXILIARES
# ==============================
def gerar_id(texto):
    return hashlib.md5(texto.encode()).hexdigest()

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("Timeout while resolving the real URL.")

# Update the extrair_conteudo function to use dynamic URL resolution and content extraction
def extrair_conteudo(link):
    try:
        # Set a timeout for resolving the real URL
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(10)  # Set timeout to 10 seconds

        # Resolve the real URL
        driver_path = "/usr/bin/chromedriver"  # Update this path as needed
        real_url = get_real_url_with_newspaper(link, driver_path)

        # Add fallback if real_url is None
        if not real_url:
            print(f"⚠️ Não consegui resolver o URL real, vou usar o link do Google News mesmo.")
            real_url = link  # Fallback to the original Google News link

        # Disable the alarm after successful resolution
        signal.alarm(0)

        if not real_url or not isinstance(real_url, str):
            print(f"⚠️ URL inválido ou não resolvido: {real_url}. Tentando usar o link original.")
            real_url = link  # Fallback to the original link

        # Extract content from the resolved URL using Selenium
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        service = Service(driver_path)
        driver = None
        try:
            driver = webdriver.Chrome(service=service, options=options)
            driver.implicitly_wait(5)  # Implicit wait for elements
            driver.get(real_url)

            # Handle consent pop-ups
            try:
                aceitar_btn = driver.find_element(By.XPATH, "//button[contains(translate(text(),'ACEITAR','aceitar'),'aceitar')]")
                aceitar_btn.click()
                print("✅ Consentimento aceite!")
            except:
                pass  # No consent button visible

            # Remove pop-ups and overlays
            driver.execute_script("""
                let modals = document.querySelectorAll('[class*="popup"], [class*="modal"], [id*="popup"]');
                modals.forEach(el => el.remove());
            """)

            # Scroll to ensure content loads
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Wait for the main content to load
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article-body, .content"))
                )
            except Exception as e:
                print(f"⚠️ Conteúdo principal não carregado: {e}")

            # Extract the main article text
            soup = BeautifulSoup(driver.page_source, "html.parser")
            article = soup.find("article") or soup.find("div", class_="article-body") or soup.find("div", class_="content")
            texto = article.get_text(separator="\n").strip() if article else soup.get_text()

            driver.quit()
            return texto
        except TimeoutException:
            if driver: driver.quit()
            print(f"⚠️ Timeout: Não foi possível resolver o URL real para o link {link} dentro do tempo limite.")
            return ""
        except Exception as e:
            if driver: driver.quit()
            print(f"❌ Erro ao extrair conteúdo: {e}")
            return ""
        finally:
            # Ensure the alarm is disabled
            signal.alarm(0)
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        return ""
    finally:
        # Ensure the alarm is disabled
        signal.alarm(0)

# Load parish codes from a JSON file
def carregar_freguesias_com_codigos(filepath):
    import json
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        print(f"❌ Erro ao carregar o arquivo {filepath}: {e}")
        return {}

# Load parish codes
FREGUESIAS_COM_CODIGOS = carregar_dicofreg("config/freguesias_com_codigos.json")

# Atualizar a função processar_item para usar o LOCALIDADES mapeado
def processar_item(item, keyword, parish):
    link = item.get("link", "")
    titulo = item.get("title", "")
    publicado = item.get("published", "")

    # Skip non-disaster-related news based on the title
    if not is_potentially_disaster_related(titulo, [keyword]):
        return None

    texto = extrair_conteudo(link)
    if not texto:
        return None

    tipo, subtipo = detect_disaster_type(texto)
    vitimas = extract_victim_counts(texto)

    # Detectar a estrutura hierárquica de localização
    loc = detect_municipality(texto, LOCALIDADES) or parish
    district, concelho = "", ""
    dicofreg = ""

    # Verificar se a localização está no LOCALIDADES mapeado
    if loc.lower() in LOCALIDADES:
        district = LOCALIDADES[loc.lower()]["district"]
        concelho = LOCALIDADES[loc.lower()]["municipality"]

    # Get the parish code (DICOFREG) if available
    parish_nome = loc.lower() if isinstance(loc, str) else ""
    parish_normalized = normalize(parish_nome)
    dicofreg = FREGUESIAS_COM_CODIGOS.get(parish_normalized, "")

    # Parse the publication date of the news
    data_evt, ano, mes, dia = parse_event_date(publicado[:10])
    hora_evt = publicado[11:16] if len(publicado) > 10 else ""  # Extract hour if available

    # Extract newspaper name and main website
    driver_path = "/usr/bin/chromedriver"  # Ensure driver_path is defined
    real_url = get_real_url_with_newspaper(link, driver_path)
    if not real_url:
        print(f"⚠️ Não consegui resolver o URL real, vou usar o link do Google News mesmo.")
        real_url = link  # Fallback to the original link

    newspaper_name = real_url.split("//")[1].split("/")[0]
    main_website = f"https://{newspaper_name}"

    return {
        "ID": gerar_id(titulo + link),
        "type": tipo,
        "subtype": subtipo,
        "date": data_evt or publicado[:10],  # Use parsed date or fallback to raw published date
        "year": ano or datetime.today().year,
        "month": mes or datetime.today().month,
        "day": dia or datetime.today().day,
        "hour": hora_evt,
        "georef": loc,
        "district": district,  # Example: "Aveiro"
        "municipali": concelho,  # Example: "Águeda"
        "parish": loc,  # Example: "Valongo do Vouga"
        "DICOFREG": dicofreg,  # Add parish code
        "source": real_url,  # Use the original link
        "sourcedate": datetime.today().date().isoformat(),  # Date when scraping was performed
        "sourcetype": newspaper_name,  # Use the newspaper name
        "page": main_website,  # Use the main website of the newspaper
        "fatalities": vitimas["fatalities"],
        "injured": vitimas["injured"],
        "evacuated": vitimas["evacuated"],
        "displaced": vitimas["displaced"],
        "missing": vitimas["missing"]
    }

def carregar_existentes():
    if os.path.exists(OUTPUT_CSV):
        df = pd.read_csv(OUTPUT_CSV)
        return set(df["ID"])
    return set()

def guardar_novos(registos):
    if not registos:
        print("Nenhuma nova notícia encontrada.")
        return

    # Convert the records to a DataFrame
    df_novo = pd.DataFrame(registos)

    # Debugging: Check if the DataFrame has the expected columns
    expected_columns = ["ID", "type", "subtype", "date", "year", "month", "day", "hour", "georef", "district", "municipali", "parish", "source", "sourcedate"]
    missing_columns = [col for col in expected_columns if col not in df_novo.columns]
    if missing_columns:
        print(f"❌ Erro: As seguintes colunas estão ausentes no DataFrame: {missing_columns}")
        return

    # Check if the output CSV already exists
    if os.path.exists(OUTPUT_CSV):
        df_existente = pd.read_csv(OUTPUT_CSV)
        df_final = pd.concat([df_existente, df_novo]).drop_duplicates(subset="ID")
    else:
        df_final = df_novo

    # Drop unnecessary columns if they exist
    if "Unnamed: 22" in df_final.columns:
        df_final = df_final.drop(columns=["Unnamed: 22"])

    # Save the final DataFrame to the CSV
    try:
        guardar_csv(OUTPUT_CSV, df_final.to_dict(orient="records"))
        print(f"✅ {len(registos)} novas notícias guardadas no arquivo {OUTPUT_CSV}.")
    except Exception as e:
        print(f"❌ Erro ao salvar o arquivo CSV: {e}")

def update_dicofreg_column(csv_filepath, json_filepath):
    """Update the DICOFREG column in the CSV file with the corresponding codigo."""
    mapping = load_freguesias_codigos(json_filepath)
    temp_filepath = Path(csv_filepath).with_suffix('.tmp')

    with open(csv_filepath, 'r', encoding='utf-8') as infile, open(temp_filepath, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            parish = row['parish']
            parish_norm = normalize(parish)
            row['DICOFREG'] = mapping.get(parish_norm, '')  # Add codigo or leave blank if not found
            writer.writerow(row)

    # Replace the original file with the updated file
    temp_filepath.replace(csv_filepath)

# ==============================
# MAIN
# ==============================
def run_scraper():
    print(f"🔍 A pesquisar combinações de {len(KEYWORDS)} palavras com {len(LOCALIDADES)} freguesias...")
    batch = []
    existentes = carregar_existentes()

    for i, keyword in enumerate(KEYWORDS):
        for j, parish in enumerate(LOCALIDADES):
            query_url = GOOGLE_NEWS_TEMPLATE.format(query=keyword.replace(" ", "+"), municipio=parish.replace(" ", "+"))
            feed = feedparser.parse(query_url)
            print(f"🔎 {keyword} em {parish} ({len(feed.entries)} notícias)")

            for item in feed.entries:
                artigo = processar_item(item, keyword, parish)
                if artigo:
                    batch.append(artigo)
                time.sleep(1)

            # Append articles to the main CSV file
            if batch:
                print(f"💾 Guardando {len(batch)} artigos no arquivo principal {OUTPUT_CSV}...")
                guardar_csv_incremental(OUTPUT_CSV, batch)
                batch.clear()

    if batch:  # Save remaining articles
        print(f"💾 Guardando {len(batch)} artigos restantes no arquivo principal {OUTPUT_CSV}...")
        guardar_csv_incremental(OUTPUT_CSV, batch)

    # Update DICOFREG column in the CSV file
    json_file = "config/freguesias_com_codigos.json"
    update_dicofreg_column(OUTPUT_CSV, json_file)

if __name__ == "__main__":
    run_scraper()
