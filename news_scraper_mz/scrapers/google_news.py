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
    parse_event_date, guardar_csv, guardar_csv_incremental
)
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# ==============================
# CONFIG
# ==============================
GOOGLE_NEWS_TEMPLATE = "https://news.google.com/rss/search?q={query}+{municipio}+Moçambique&hl=pt-PT&gl=MZ&ceid=MZ:pt-150"
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")
LOCALIDADES = load_localidades("config/mozambique_admin_structure.json")

OUTPUT_CSV = "data/artigos_google_municipios_mz.csv"
HEADERS = {"User-Agent": "MunicipioNewsBot/1.0"}

# ==============================
# FUNCOES AUXILIARES
# ==============================
def gerar_id(texto):
    return hashlib.md5(texto.encode()).hexdigest()

def extrair_conteudo(link):
    try:
        html = requests.get(link, headers=HEADERS, timeout=10).text
        soup = BeautifulSoup(html, "html.parser")
        texto = extract_article_text(soup)
        return texto.strip()
    except:
        return ""

def processar_item(item, keyword, municipio):
    link = item.get("link", "")
    titulo = item.get("title", "")
    publicado = item.get("published", "")
    texto = extrair_conteudo(link)

    if not texto or not is_potentially_disaster_related(texto, [keyword]): #DEBUG:
        return None

    tipo, subtipo = detect_disaster_type(texto)
    vitimas = extract_victim_counts(texto)
    loc = detect_municipality(texto, LOCALIDADES) or municipio

    data_evt, ano, mes, dia = parse_event_date(publicado[:10])
    hora_evt = ""

    return {
        "ID": gerar_id(titulo + link),
        "type": tipo,
        "subtype": subtipo,
        "date": data_evt or datetime.today().date().isoformat(),
        "year": ano or datetime.today().year,
        "month": mes or datetime.today().month,
        "day": dia or datetime.today().day,
        "hour": hora_evt,
        "georef": loc,
        "district": "",
        "municipali": loc,
        "parish": "",
        "DICOFREG": "",
        "source": link,
        "sourcedate": publicado,
        "sourcetype": "google_news",
        "page": link,
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
    if os.path.exists(OUTPUT_CSV):
        df_existente = pd.read_csv(OUTPUT_CSV)
        df_novo = pd.DataFrame(registos)
        df_final = pd.concat([df_existente, df_novo]).drop_duplicates(subset="ID")
    else:
        df_final = pd.DataFrame(registos)
    guardar_csv(OUTPUT_CSV, df_final.to_dict(orient="records"))
    print(f"✅ {len(registos)} novas notícias guardadas.")

# ==============================
# MAIN
# ==============================
def run_scraper():
    print(f"🔍 A pesquisar combinações de {len(KEYWORDS)} palavras com {len(LOCALIDADES)} municípios...")
    batch = []
    existentes = carregar_existentes()

    for i, keyword in enumerate(KEYWORDS):
        for j, municipio in enumerate(LOCALIDADES):
            query_url = GOOGLE_NEWS_TEMPLATE.format(query=keyword.replace(" ", "+"), municipio=municipio.replace(" ", "+"))
            feed = feedparser.parse(query_url)
            print(f"🔎 {keyword} em {municipio} ({len(feed.entries)} notícias)")

            for item in feed.entries:
                artigo = processar_item(item, keyword, municipio)
                if artigo:
                    batch.append(artigo)
                time.sleep(1)

            if (j + 1) % 10 == 0:  # Save every 10 cities
                print(f"💾 Guardando {len(batch)} artigos...")
                guardar_csv_incremental(OUTPUT_CSV, batch)
                batch.clear()

    if batch:  # Save remaining articles
        print(f"💾 Guardando {len(batch)} artigos restantes...")
        guardar_csv_incremental(OUTPUT_CSV, batch)

if __name__ == "__main__":
    run_scraper()
