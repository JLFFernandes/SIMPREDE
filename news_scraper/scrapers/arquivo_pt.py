import os
import sys
import uuid
import requests
import time
from datetime import datetime
from bs4 import BeautifulSoup
import csv

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils import helpers

# -----------------------------
# Configurações
# -----------------------------
KEYWORDS = helpers.load_keywords("config/keywords.json")
MUNICIPIOS = helpers.load_localidades("config/municipios.json")
DATA_INICIO = "20200101000000"
DATA_FIM = "20241231000000"
DELAY = 1  # segundos entre requests
MAX_ITEMS = 500  # máximo permitido pela API

# -----------------------------
# Função principal de scraping
# -----------------------------
def scrape(site: str) -> list[dict]:
    print(f"\n📰 A processar artigos de: {site}")
    artigos_encontrados = []
    vistos = set()

    keywords = [helpers.normalize(k) for k in KEYWORDS]
    municipios = MUNICIPIOS
    municipios = municipios[:3]  # limitar para testes

    combinacoes = [f"{kw} {loc}" for kw in keywords for loc in municipios]

    for query in combinacoes:
        offset = 0
        while True:
            url_api = (
                f"https://arquivo.pt/textsearch?q={query}"
                f"&from={DATA_INICIO}&to={DATA_FIM}&siteSearch={site}&maxItems={MAX_ITEMS}&offset={offset}"
            )
            print(f"🔎 Buscando: {url_api}")
            try:
                response = requests.get(url_api, timeout=15)
                response.raise_for_status()
                data = response.json()
                resultados = data.get("response_items", [])
                if not resultados:
                    break
                offset += MAX_ITEMS
            except Exception as e:
                print(f"❌ Erro ao buscar '{query}': {e}")
                break

            time.sleep(DELAY)
            print(f"🔍 Resultados: {len(resultados)} para '{query}'")

            for item in resultados:
                title = item.get("title", "").lower()
                if any(x in title for x in ["video", "multimedia", "topicos", "galeria", "fotos", "opinião", "comentários"]):
                    continue

                link1 = item.get("linkToExtractedText")
                link2 = item.get("linkToArchive")
                data_evento = item.get("date")

                texto = ""
                for link in [link1, link2]:
                    if not link:
                        continue
                    try:
                        r = requests.get(link, timeout=15)
                        if "textextracted" in link:
                            texto = helpers.limpar_texto_lixo(r.text.strip())
                        else:
                            soup = BeautifulSoup(r.text, "html.parser")
                            texto = helpers.extract_article_text(soup)
                        if texto:
                            break
                    except Exception as e:
                        print(f"⚠️ Falha ao aceder {link}: {e}")
                        continue

                if not texto or len(texto) < 100:
                    print(f"⚠️ Texto vazio ou demasiado curto para: {link1 or link2}")
                    continue

                titulo_extraido = getattr(helpers, 'extract_title_from_text', lambda x: None)(texto)
                titulo = titulo_extraido or item.get("title", "").strip()

                if not helpers.is_in_portugal(titulo, texto, municipios):
                    continue

                vitimas = helpers.extract_victim_counts(texto)
                hora_evento = helpers.extract_event_hour(texto)
                tipo_risco, subtipo_risco = helpers.detect_disaster_type(texto)
                municipio = helpers.detect_municipality(texto, municipios)

                data_str = datetime.utcfromtimestamp(int(data_evento)).strftime('%Y-%m-%d') if data_evento else None
                _, ano, mes, dia = helpers.parse_event_date(data_str) if data_str else (None, None, None, None)

                if not helpers.is_potentially_disaster_related(texto):
                    continue
                if not municipio:
                    continue

                artigo = {
                    "ID": str(uuid.uuid4()),
                    "type": tipo_risco,
                    "subtype": subtipo_risco,
                    "date": data_str,
                    "year": ano,
                    "month": mes,
                    "day": dia,
                    "hour": hora_evento,
                    "georef": None,
                    "district": None,
                    "municipali": municipio,
                    "parish": None,
                    "DICOFREG": None,
                    "source": f"Arquivo - {site}",
                    "sourcedate": data_str,
                    "sourcetype": "diário",
                    "page": None,
                    **vitimas,
                    "DataScraping": datetime.now().isoformat(),
                    "link_extraido": link if texto else None
                }
                artigos_encontrados.append(artigo)

        print(f"📦 Total bruto para {site}: {len(artigos_encontrados)} artigos")
        print(f"✅ {site}: {len(artigos_encontrados)} artigos únicos encontrados.")

    return artigos_encontrados


# -----------------------------
# Exportação para CSV com ordem das colunas
# -----------------------------
def exportar_csv(artigos: list[dict], ficheiro: str):
    colunas = [
        "ID", "type", "subtype", "date", "year", "month", "day", "hour",
        "georef", "district", "municipali", "parish", "DICOFREG",
        "source", "sourcedate", "sourcetype", "page",
        "fatalities", "injured", "evacuated", "displaced", "missing",
        "DataScraping", "link_extraido"
    ]
    with open(ficheiro, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        for artigo in artigos:
            writer.writerow({col: artigo.get(col) for col in colunas})