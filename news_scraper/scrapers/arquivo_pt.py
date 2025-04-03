import os
import sys
import uuid
import requests
import time
from datetime import datetime
from bs4 import BeautifulSoup
import csv
import json
from utils.helpers import normalize
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils import helpers

# -----------------------------
# Configurações
# -----------------------------
# Replace helpers.load_keywords with an inline implementation
def load_keywords(filepath: str, idioma: str = "portuguese") -> list[str]:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("weather_terms", {}).get(idioma, [])

# Replace helpers.load_municipios_distritos with an inline implementation
def load_municipios_distritos(filepath: str) -> list[dict]:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

# Update KEYWORDS and MUNICIPIOS to use the new functions
KEYWORDS = load_keywords("config/keywords.json")

# Update MUNICIPIOS to flatten the structure and extract all municipalities
MUNICIPIOS = [
    municipio
    for distrito in load_municipios_distritos("config/municipios_por_distrito.json").values()
    for municipio in distrito.keys()
]

DATA_INICIO = "20190101000000"
DATA_FIM = "20191231000000"
DELAY = 1  # segundos entre requests
MAX_ITEMS = 500  # máximo permitido pela API

# -----------------------------
# Função principal de scraping
# -----------------------------
def scrape(site: str) -> list[dict]:
    print(f"\n📰 A processar artigos de: {site}")
    artigos_encontrados = []

    keywords = KEYWORDS
    municipios = MUNICIPIOS

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
                title = item.get("title", "").strip()
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

                data_str = datetime.utcfromtimestamp(int(data_evento)).strftime('%Y-%m-%d') if data_evento else None

                artigo = {
                    "ID": str(uuid.uuid4()),
                    "title": title,
                    "date": data_str,
                    "source": f"Arquivo - {site}",
                    "link_extraido": link1 or link2,
                    "fulltext": texto,
                    "sourcetype": "arquivo"
                }
                artigos_encontrados.append(artigo)

        print(f"📦 Total bruto para {site}: {len(artigos_encontrados)} artigos")

    return artigos_encontrados


# -----------------------------
# Exportação para CSV simples
# -----------------------------
def exportar_csv(artigos: list[dict], ficheiro: str):
    colunas = ["ID", "title", "date", "source", "link_extraido", "fulltext", "sourcetype"]
    with open(ficheiro, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        for artigo in artigos:
            writer.writerow({col: artigo.get(col) for col in colunas})
