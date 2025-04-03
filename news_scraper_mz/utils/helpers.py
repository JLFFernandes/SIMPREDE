import unicodedata
import pandas as pd
import re
from datetime import datetime
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
import json
import csv
import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


# ----------------------------- Normalização -----------------------------
def normalize(text):
    return unicodedata.normalize('NFKD', text.lower()).encode('ASCII', 'ignore').decode('utf-8').strip()

# --------------------------- Carregamento do mapa de localizações ---------------------------
with open("config/mozambique_admin_structure.json", "r", encoding="utf-8") as f:
    raw_data = json.load(f)

MAPA_LOCALIZACOES = []
for provincia, distritos in raw_data.items():
    for distrito, postos in distritos.items():
        for posto in postos:
            MAPA_LOCALIZACOES.append({
                "provincia": provincia,
                "distrito": distrito,
                "municipio": posto
            })
        MAPA_LOCALIZACOES.append({
            "provincia": provincia,
            "distrito": distrito,
            "municipio": distrito
        })


def load_municipios_distritos(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def detect_municipality(texto, localidades):
    texto_norm = normalize(texto)
    for mun in localidades:
        if normalize(mun) in texto_norm:
            return mun
    return None


def load_keywords(path, idioma="portuguese"):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data.get("weather_terms"), dict):
        return data["weather_terms"].get(idioma, [])
    return data.get(idioma, [])


def load_localidades(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    localidades = []
    for provincia, distritos in data.items():
        for distrito, postos in distritos.items():
            localidades.extend(postos)
            localidades.append(distrito)
    return localidades


# --------------------------- Localização ---------------------------
def verificar_localizacao(texto):
    texto_norm = normalize(texto)
    for item in MAPA_LOCALIZACOES:
        mun = normalize(item["municipio"])
        if mun in texto_norm:
            return {
                "municipali": item["municipio"].upper(),
                "district": item["distrito"].upper(),
                "parish": None,
                "dicofreg": None,
                "georef": "location based on text scraping"
            }
    return None

# --------------------------- Verificação ---------------------------
def is_in_portugal(title: str, article_text: str) -> bool:
    return False  # Moçambique only, desativa validação de Portugal

# --------------------------- Extrações ---------------------------
def extract_title_from_text(texto: str) -> str:
    linhas = texto.splitlines()
    candidatas = [linha.strip() for linha in linhas if 15 < len(linha.strip()) < 120]
    return candidatas[0] if candidatas else ""


def parse_event_date(date_str):
    if not date_str:
        return None, None, None, None
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        return date_obj, date_obj.year, date_obj.month, date_obj.day
    except ValueError:
        return None, None, None, None


def extract_event_hour(text: str) -> str | None:
    text = text.lower()
    match = re.search(r'(\d{1,2})h(\d{1,2})?', text)
    if match:
        return match.group(0)
    for periodo in ["madrugada", "manha", "tarde", "noite"]:
        if periodo in text:
            return periodo
    return None

# --------------------------- Tipo de desastre ---------------------------
def detect_disaster_type(text: str) -> tuple[str, str]:
    text = normalize(text)
    categorias = {
        "Flood": ["cheia", "inundacao", "alagamento", "transbordo"],
        "Landslide": ["deslizamento", "desabamento", "desmoronamento", "queda de terra"]
    }
    for tipo, termos in categorias.items():
        for termo in termos:
            if termo in text:
                return tipo, termo
    return "Other", "Other"

# --------------------------- Vítimas ---------------------------
def extract_victim_counts(text: str) -> dict:
    text = text.lower()
    results = {"fatalities": 0, "injured": 0, "evacuated": 0, "displaced": 0, "missing": 0}
    patterns = {
        "fatalities": [r"(\d+)\s+(mort[oa]s?|faleceram|morreram)"],
        "injured": [r"(\d+)\s+(ferid[oa]s?)"],
        "evacuated": [r"(\d+)\s+(evacuad[oa]s?)"],
        "displaced": [r"(\d+)\s+(desalojad[oa]s?)"],
        "missing": [r"(\d+)\s+(desaparecid[oa]s?)"]
    }
    for key, regexes in patterns.items():
        for pattern in regexes:
            match = re.search(pattern, text)
            if match:
                results[key] = int(match.group(1))
    return results

# --------------------------- Relevância ---------------------------
def is_potentially_disaster_related(text: str, keywords: list[str]) -> bool:
    # Accept all texts as potentially disaster-related
    return True

# --------------------------- Limpeza de texto bruto ---------------------------
def limpar_texto_lixo(texto: str) -> str:
    if not texto:
        return ""

    # Remove repetitive beginning text using regex for variations
    repetitive_patterns = [
        r"we usecookiesand data to.*?if you choose to “accept all,”",  # Match variations of the repetitive text
        r"we will also use cookies and data to.*?if you"  # Additional pattern for variations
    ]
    for pattern in repetitive_patterns:
        texto = re.sub(pattern, "", texto, flags=re.IGNORECASE).strip()

    # Debugging: Print the cleaned text for verification
    print(f"🔍 Texto após limpeza inicial: {texto[:100]}...")

    # Remove other predefined "lixo"
    lixo = [
        "Saltar para o conteudo", "Este site utiliza cookies", "Iniciar sessao",
        "Politica de Privacidade", "Publicidade", "Registe-se", "Assine ja",
        "Versao Normal", "Mais populares", "Ultimas Noticias", "Facebook",
        "Google+", "RSS"
    ]
    linhas = texto.splitlines()
    filtradas = [linha for linha in linhas if not any(lixo_item in normalize(linha) for lixo_item in lixo)]
    return "\n".join(filtradas).strip()

# --------------------------- Extração de artigo de HTML ---------------------------
def extract_article_text(soup):
    containers = [
        soup.find('article'),
        soup.find('main'),
        soup.find('div', class_='article-body'),
        soup.find('div', class_='story'),
        soup.find('body')
    ]
    for container in containers:
        if container:
            paragraphs = container.find_all('p')
            texto = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
            if texto:
                return " ".join(texto).strip()
    # fallback
    paragraphs = soup.find_all('p')
    texto = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
    return " ".join(texto).strip()

def fetch_and_extract_article_text(url: str) -> str:
    """
    Fetches the content of a webpage and extracts the main article text.
    """
    import requests
    try:
        print(f"🌐 Fetching URL: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for HTTP issues
        print(f"✅ Successfully fetched URL: {url}")

        soup = BeautifulSoup(response.text, "html.parser")

        # Attempt to extract the main article text
        containers = [
            soup.find('article'),
            soup.find('main'),
            soup.find('div', class_='article-body'),
            soup.find('div', class_='story'),
            soup.find('div', class_='content'),
            soup.find('body')
        ]
        for container in containers:
            if container:
                paragraphs = container.find_all('p')
                text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
                if text:
                    print(f"🔍 Extracted text from container: {container.name}")
                    return " ".join(text).strip()

        # Fallback: Try extracting all <p> tags if no container is found
        paragraphs = soup.find_all('p')
        text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
        if text:
            print(f"🔍 Extracted text from fallback <p> tags.")
            return " ".join(text).strip()

        # Final fallback: Extract text from all visible elements
        visible_text = soup.stripped_strings
        combined_text = " ".join(visible_text)
        if combined_text:
            print(f"🔍 Extracted text from all visible elements.")
            return combined_text.strip()

        print(f"⚠️ No article or content found for URL: {url}")
        return ""

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error fetching URL {url}: {e}")
        return ""
    except Exception as e:
        print(f"⚠️ Unexpected error for URL {url}: {e}")
        return ""

def fetch_and_extract_article_text_dynamic(url: str) -> str:
    """
    Fetches the content of a webpage using Selenium and extracts the main article text.
    """
    try:
        print(f"🌐 Fetching URL dynamically: {url}")
        options = Options()
        options.add_argument("--headless")  # Run in headless mode
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        service = Service("/path/to/chromedriver")  # Update with the path to your ChromeDriver
        driver = webdriver.Chrome(service=service, options=options)

        driver.get(url)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()

        # Attempt to extract the main article text
        containers = [
            soup.find('article'),
            soup.find('main'),
            soup.find('div', class_='article-body'),
            soup.find('div', class_='story'),
            soup.find('div', class_='content'),
            soup.find('body')
        ]
        for container in containers:
            if container:
                paragraphs = container.find_all('p')
                text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
                if text:
                    print(f"🔍 Extracted text from container: {container.name}")
                    return " ".join(text).strip()

        # Fallback: Try extracting all <p> tags if no container is found
        paragraphs = soup.find_all('p')
        text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
        if text:
            print(f"🔍 Extracted text from fallback <p> tags.")
            return " ".join(text).strip()

        print(f"⚠️ No article or content found for URL: {url}")
        return ""

    except Exception as e:
        print(f"⚠️ Error fetching URL dynamically {url}: {e}")
        return ""

# --------------------------- Exportar eventos com vítimas ---------------------------
def guardar_disaster_db_ready(artigos: list[dict], ficheiro: str):
    colunas = [
        "ID", "title", "date", "type", "subtype", "district", "municipali",
        "parish", "dicofreg", "hour", "source", "link_extraido", "fatalities",
        "injured", "evacuated", "displaced", "missing", "sourcetype"
    ]
    artigos_filtrados = [
        a for a in artigos
        if any(a.get(chave, 0) > 0 for chave in ["fatalities", "injured", "evacuated", "displaced", "missing"])
    ]
    if not artigos_filtrados:
        print("⚠️ Nenhum artigo com vítimas encontrado.")
        return
    with open(ficheiro, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        for artigo in artigos_filtrados:
            writer.writerow({col: artigo.get(col, "") for col in colunas})

# --------------------------- Guardar CSV genérico ---------------------------
def guardar_csv(caminho, artigos: list[dict]):
    if not artigos:
        print("⚠️ Nenhum artigo para guardar.")
        return
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=artigos[0].keys())
        writer.writeheader()
        for artigo in artigos:
            writer.writerow(artigo)
    print(f"✅ Guardado com sucesso em {caminho}")

# --------------------------- Guardar incremental ---------------------------
def guardar_csv_incremental(caminho, novos_artigos: list[dict]):
    if not novos_artigos:
        print("⚠️ Nenhum novo artigo recebido.")
        return

    print(f"Novos artigos recebidos: {len(novos_artigos)}")

    # Ensure all articles have valid IDs
    novos_artigos = [a for a in novos_artigos if a.get("ID")]
    if not novos_artigos:
        print("⚠️ Nenhum artigo com ID válido encontrado.")
        return

    # Check for existing articles in the file
    if os.path.exists(caminho):
        with open(caminho, "r", encoding="utf-8") as f:
            existentes = {row["ID"] for row in csv.DictReader(f)}
    else:
        existentes = set()

    print(f"IDs existentes no arquivo: {existentes}")

    # Filter out articles with duplicate IDs
    artigos_unicos = [a for a in novos_artigos if a["ID"] not in existentes]
    print(f"Artigos únicos para salvar: {len(artigos_unicos)}")

    if not artigos_unicos:
        print("⚠️ Nenhum artigo único para salvar.")
        return

    # Append articles to the single CSV file in batches of 10
    with open(caminho, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=artigos_unicos[0].keys())
        if os.stat(caminho).st_size == 0:  # Write header if file is empty
            writer.writeheader()

        batch_size = 10
        for i in range(0, len(artigos_unicos), batch_size):
            batch = artigos_unicos[i:i + batch_size]
            for artigo in batch:
                writer.writerow(artigo)
            print(f"💾 Guardados {len(batch)} artigos no arquivo {caminho} (batch {i // batch_size + 1})")