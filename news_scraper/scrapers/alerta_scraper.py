import feedparser
import time
import os
import sys
from datetime import datetime
from utils.helpers import load_keywords


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from extracao.extractor import extrair_conteudo
from utils.helpers import guardar_csv_incremental
from extracao.normalizador import (
    extract_victim_counts,
    detect_disaster_type,
    parse_event_date,
    is_potentially_disaster_related
)

OUTPUT_CSV = "data/artigos_alerta_google.csv"
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")

def extrair_artigos_do_alerta(feed_url):
    """
    Extrai os artigos de um feed do Google Alerts RSS.
    """
    artigos = []
    feed = feedparser.parse(feed_url)
    for entry in feed.entries:
        artigos.append({
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "summary": entry.get("summary", "")
        })
    return artigos

def processar_artigo_alerta(entrada):
    """
    Processa um artigo individual do feed do Google Alerts.
    """
    titulo = entrada["title"]
    link = entrada["link"]
    publicado = entrada["published"]

    if not is_potentially_disaster_related(titulo, KEYWORDS):
        return None

    texto = extrair_conteudo(link)
    if not texto or len(texto.split()) < 50:
        print(f"⚠️ Conteúdo insuficiente: {link}")
        return None

    tipo, subtipo = detect_disaster_type(texto)
    vitimas = extract_victim_counts(texto)
    data_evt, ano, mes, dia = parse_event_date(publicado[:10])
    hora_evt = publicado[11:16] if len(publicado) > 10 else ""

    artigo = {
        "ID": hash(link),
        "type": tipo,
        "subtype": subtipo,
        "date": data_evt or publicado[:10],
        "year": ano or datetime.today().year,
        "month": mes or datetime.today().month,
        "day": dia or datetime.today().day,
        "hour": hora_evt,
        "georef": "",
        "district": "",
        "municipali": "",
        "parish": "",
        "DICOFREG": "",
        "source": link,
        "sourcedate": datetime.today().isoformat(),
        "sourcetype": "GoogleAlert",
        "page": link.split("/")[2],
        "fatalities": vitimas["fatalities"],
        "injured": vitimas["injured"],
        "evacuated": vitimas["evacuated"],
        "displaced": vitimas["displaced"],
        "missing": vitimas["missing"],
    }

    return artigo

def run_alerta_scraper(feed_url):
    artigos_brutos = extrair_artigos_do_alerta(feed_url)
    batch = []
    for entrada in artigos_brutos:
        artigo = processar_artigo_alerta(entrada)
        if artigo:
            batch.append(artigo)
            print(f"✅ Artigo processado: {artigo['title']}")
        time.sleep(1)

    if batch:
        guardar_csv_incremental(OUTPUT_CSV, batch)
        print(f"📦 {len(batch)} artigos guardados em {OUTPUT_CSV}.")
