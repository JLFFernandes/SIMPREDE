import feedparser
import requests
import time
from bs4 import BeautifulSoup
import sys
import os
from utils.helpers import (
    extract_article_text, extract_title_from_text, extract_victim_counts,
    detect_disaster_type, detect_municipality, load_keywords,
    load_localidades, is_potentially_disaster_related,
    parse_event_date, guardar_csv, normalize
)
import pandas as pd
from datetime import datetime
import hashlib

# ==============================
# CONFIG
# ==============================
RSS_FEEDS = [
    "https://feeds.feedburner.com/PublicoRSS"
]

KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")
LOCALIDADES = load_localidades("config/municipios.json")

print(f"🔍 {len(KEYWORDS)} palavras-chave carregadas:") #DEBUG:
print(KEYWORDS) #DEBUG:

OUTPUT_CSV = "data/artigos_filtrados.csv"
HEADERS = {"User-Agent": "Projeto_Final_UAB/1.0 (mailto:teu@email.pt)"}

# ==============================
# FUNÇÕES
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

def processar_item(item):
    link = item.get("link", "")
    titulo = item.get("title", "")
    publicado = item.get("published", "")
    texto = extrair_conteudo(link)

    if not texto or not is_potentially_disaster_related(texto, KEYWORDS):
        if not texto:
            print(f"🕳️ Ignorado (sem conteúdo): {titulo} ({link})")
        else:
            print(f"❌ Ignorado (não relevante): {titulo}")
        return None
    else:
        print(f"✅ RELEVANTE: {titulo}")

    tipo, subtipo = detect_disaster_type(texto)
    vitimas = extract_victim_counts(texto)
    municipio = detect_municipality(texto, LOCALIDADES) or ""
    georef = municipio if municipio else ""
    distrito = ""

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
        "georef": georef,
        "district": distrito,
        "municipali": municipio,
        "parish": "",
        "DICOFREG": "",
        "source": link,
        "sourcedate": publicado,
        "sourcetype": "rss",
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
        print("Nenhuma nova notícia relevante encontrada.")
        return

    if os.path.exists(OUTPUT_CSV):
        df_existente = pd.read_csv(OUTPUT_CSV)
        df_novo = pd.DataFrame(registos)
        df_final = pd.concat([df_existente, df_novo]).drop_duplicates(subset="ID")
    else:
        df_final = pd.DataFrame(registos)

    guardar_csv(OUTPUT_CSV, df_final.to_dict(orient="records"))
    print(f"✅ {len(registos)} novas notícias adicionadas ao ficheiro.")

# ==============================
# MAIN
# ==============================
def run_scraper():
    print("\n🚨 A recolher notícias relevantes...")
    novos = []
    existentes = carregar_existentes()

    # RSS normais
    for url in RSS_FEEDS:
        print(f"📰 A ler RSS: {url}")
        feed = feedparser.parse(url)
        for item in feed.entries:
            print(f"🔗 A processar: {item.get('link', 'sem link')}")
            artigo = processar_item(item)
            if artigo and artigo["ID"] not in existentes:
                novos.append(artigo)
            time.sleep(1)

    guardar_novos(novos)

if __name__ == "__main__":
    run_scraper()