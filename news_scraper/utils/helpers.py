import unicodedata
import pandas as pd
import re
from datetime import datetime
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
import json
import csv

# ----------------------------- Normalização -----------------------------
def normalize(text):
    return unicodedata.normalize('NFKD', text.lower()).encode('ASCII', 'ignore').decode('utf-8').strip()

# --------------------------- Carregamento do mapa de localizações ---------------------------
with open("config/municipios_por_distrito.json", "r", encoding="utf-8") as f:
    raw_data = json.load(f)

MAPA_LOCALIZACOES = []
for distrito, municipios in raw_data.items():
    for municipio in municipios:
        MAPA_LOCALIZACOES.append({
            "distrito": distrito,
            "municipio": municipio
        })

def load_municipios_distritos(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# --------------------------- Localização ---------------------------
def verificar_localizacao(texto):
    texto_norm = normalize(texto)
    for item in MAPA_LOCALIZACOES:
        mun = normalize(item["municipio"])
        if mun in texto_norm:
            return {
                "municipali": item["municipio"].upper(),
                "district": item["distrito"].upper(),
                "parish": item.get("freguesia", None),
                "dicofreg": item.get("dicofreg", None),
                "georef": "location based on text scraping"
            }
    return None

# --------------------------- Verificação ---------------------------
def is_in_portugal(title: str, article_text: str) -> bool:
    content = normalize(f"{title} {article_text}")
    for item in MAPA_LOCALIZACOES:
        if normalize(item["municipio"]) in content:
            return True
    return False

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
def is_potentially_disaster_related(text: str) -> bool:
    termos = ["cheia", "inundação", "derrocada", "deslizamento", "inundações", "desabamento"]
    text_norm = normalize(text)
    return any(t in text_norm for t in termos)

# --------------------------- Limpeza de texto bruto ---------------------------
def limpar_texto_lixo(texto: str) -> str:
    if not texto:
        return ""
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
