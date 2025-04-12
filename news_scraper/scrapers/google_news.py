import urllib.request
from operator import is_
import feedparser
import requests
import time
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import sys
import os
import json
import csv
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from extracao.normalizador import (
    extract_victim_counts,
    detect_disaster_type,
    is_potentially_disaster_related,
    normalize,
    parse_event_date,
)
from extracao.extractor import (
    fetch_and_extract_article_text,
    extrair_conteudo,
    resolve_google_news_url,
    load_freguesias_codigos,
    fetch_and_extract_article_text_dynamic,
)
from utils.helpers import (
    guardar_csv_incremental, 
    guardar_csv,
    load_keywords,
    carregar_paroquias_com_municipios,
    process_urls_in_parallel,
    detect_municipality,
    execute_in_parallel
)


# ==============================
# CONFIG
# ==============================
GOOGLE_NEWS_TEMPLATE = "https://news.google.com/rss/search?q={query}+{municipio}+Portugal&hl=pt-PT&gl=PT&ceid=PT:pt"

KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")
LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")

# Load the mapping of parishes to codes (FREGUESIAS_COM_CODIGOS)
with open("config/freguesias_com_codigos.json", "r", encoding="utf-8") as f:
    FREGUESIAS_COM_CODIGOS = json.load(f)

OUTPUT_CSV = "data/artigos_google_municipios_pt.csv"
HEADERS = {"User-Agent": "Final_uni_project/1.0"}

INTERMEDIATE_CSV = "data/intermediate_google_news.csv"


# ==============================
# FUNCOES AUXILIARES
# ==============================
def gerar_id(titulo, real_url, texto):
    """
    Generate a unique ID for an article based on its title, real URL, and content.
    """
    if not titulo or not real_url or not texto:
        return None
    return hashlib.md5((real_url + texto[:300]).encode("utf-8")).hexdigest()

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("Timeout while resolving the real URL.")


# Atualizar a função processar_item para usar o LOCALIDADES mapeado
def processar_item(item, keyword, parish, texto=None):
    link = item.get("link", "")
    titulo = item.get("title", "")
    publicado = item.get("published", "")

    if not link or not link.startswith("http"):
        print(f"⚠️ Link inválido: {link}")
        return None

    if texto is None:
        texto = extrair_conteudo(link)

    if not texto or len(texto.split()) < 50:
        print(f"⚠️ Conteúdo insuficiente para o link: {link}")
        return None

    # Verificação extra no conteúdo completo
    if not is_potentially_disaster_related(texto, [keyword]):
        print(f"⚠️ Artigo ignorado por irrelevância no conteúdo: {titulo}")
        return None

    tipo, subtipo = detect_disaster_type(texto)
    vitimas = extract_victim_counts(texto)

    loc = detect_municipality(texto, LOCALIDADES) or parish
    district, concelho = "", ""
    dicofreg = ""

    if loc.lower() in LOCALIDADES:
        district = LOCALIDADES[loc.lower()]["district"]
        concelho = LOCALIDADES[loc.lower()]["municipality"]

    parish_nome = loc.lower() if isinstance(loc, str) else ""
    parish_normalized = normalize(parish_nome)
    dicofreg = FREGUESIAS_COM_CODIGOS.get(parish_normalized, "")

    data_evt, ano, mes, dia = parse_event_date(publicado[:10])
    hora_evt = publicado[11:16] if len(publicado) > 10 else ""

    real_url = item.get("link", "")
    newspaper_name = real_url.split("//")[1].split("/")[0]
    main_website = f"https://{newspaper_name}"

    article_id = gerar_id(titulo, real_url, texto)
    if not article_id:
        return None

    artigo = {
        "ID": article_id,
        "type": tipo,
        "subtype": subtipo,
        "date": data_evt or publicado[:10],
        "year": ano or datetime.today().year,
        "month": mes or datetime.today().month,
        "day": dia or datetime.today().day,
        "hour": hora_evt,
        "georef": loc,
        "district": district,
        "municipali": concelho,
        "parish": loc,
        "DICOFREG": dicofreg,
        "source": real_url,
        "sourcedate": datetime.today().date().isoformat(),
        "sourcetype": newspaper_name,
        "page": main_website,
        "fatalities": vitimas["fatalities"],
        "injured": vitimas["injured"],
        "evacuated": vitimas["evacuated"],
        "displaced": vitimas["displaced"],
        "missing": vitimas["missing"]
    }

    try:
        guardar_csv_incremental(OUTPUT_CSV, [artigo])
        print(f"✅ Artigo salvo: {titulo} ({parish})")
    except Exception as e:
        print(f"❌ Erro ao salvar o artigo no arquivo CSV: {e}")

    return artigo


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

def fetch_feed_parallel(keyword_parish_pairs, max_threads=10):
    """
    Busca feeds RSS do Google News em paralelo para combinações de palavras-chave e freguesias.
    """
    def fetch_feed(pair):
        keyword, parish = pair
        query_url = GOOGLE_NEWS_TEMPLATE.format(
            query=keyword.replace(" ", "+"), municipio=parish.replace(" ", "+")
        )
        feed = feedparser.parse(query_url)
        print(f"🔎 {keyword} em {parish} ({len(feed.entries)} notícias)")
        return (keyword, parish, feed.entries)

    results = []
    with ThreadPoolExecutor(max_threads) as executor:
        futures = {executor.submit(fetch_feed, pair): pair for pair in keyword_parish_pairs}

        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"❌ Erro ao buscar feed para {futures[future]}: {e}")

    return results

def save_intermediate_links(links):
    """
    Save intermediate links and metadata to a CSV file.
    """
    if not links:
        print("⚠️ Nenhum link para salvar no arquivo intermediário.")
        return

    # Ensure the directory exists
    os.makedirs(os.path.dirname(INTERMEDIATE_CSV), exist_ok=True)

    # Save links to the intermediate CSV
    with open(INTERMEDIATE_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=links[0].keys())
        writer.writeheader()
        writer.writerows(links)
    print(f"✅ Links intermediários salvos em INTERMEDIATE_CSV.")

def processar_artigos_em_lote(feed_entries, keyword, parish, max_threads=4):
    """
    Processa artigos em paralelo para extrair links reais e textos.
    """
    def worker(item):
        return processar_item(item, keyword, parish)
    
    return execute_in_parallel(feed_entries, worker, max_threads)

def processar_links_em_lote(feed_entries, max_workers=4):
    """
    Resolve os links reais em paralelo usando process_urls_in_parallel.
    """
    urls = [item.get("link", "") for item in feed_entries if item.get("link", "").startswith("http")]
    if not urls:
        print("⚠️ Nenhum URL válido encontrado para processar.")
        return []
    return process_urls_in_parallel(urls, fetch_and_extract_article_text, max_workers=max_workers)

def run_scraper():
    print(f"🔍 A pesquisar combinações de {len(KEYWORDS)} palavras com {len(LOCALIDADES)} freguesias, municípios e distritos...")

    TODAS_LOCALIDADES = list(LOCALIDADES.keys()) + MUNICIPIOS + DISTRITOS
    intermediate_links_raw = []
    batch = []
    total_articles = 0

    # 1️⃣ Fase 1: apenas guardar links brutos de RSS
    for keyword in KEYWORDS:
        for i, localidade in enumerate(TODAS_LOCALIDADES, 1):  # começa no 1
            query_url = GOOGLE_NEWS_TEMPLATE.format(
                query=keyword.replace(" ", "+"),
                municipio=localidade.replace(" ", "+")
            )
            feed = feedparser.parse(query_url)
            print(f"🔎 {keyword} em {localidade} ({len(feed.entries)} notícias)")

            tipo = "freguesia" if localidade.lower() in LOCALIDADES else "municipio" if localidade in MUNICIPIOS else "distrito"

            for item in feed.entries:
                uid = hashlib.md5((item.get("title", "") + item.get("link", "")).encode()).hexdigest()
                intermediate_links_raw.append({
                    "ID": uid,
                    "localidade": localidade,
                    "title": item.get("title", ""),
                    "link": item.get("link", "")
                })


            # Salva a cada 10 localidades
            if i % 10 == 0:
                try:
                    with open(INTERMEDIATE_CSV, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=["ID", "localidade", "title", "link"])
                        writer.writeheader()
                        writer.writerows(intermediate_links_raw)
                    print(f"💾 Parcial: {len(intermediate_links_raw)} links intermédios salvos após {i} localidades.")
                except Exception as e:
                    print(f"❌ Erro ao salvar parcialmente os links intermédios: {e}")



    # 3️⃣ Fase 2: processar os links resolvidos e filtrar artigos relevantes
    for item in intermediate_links_raw:
        url_original = item.get("link", "")
        resolved_url = resolve_google_news_url(url_original)

        if not resolved_url or not resolved_url.startswith("http"):
            print(f"⚠️ URL resolvido inválido: {resolved_url}")
            continue

        item["link"] = resolved_url
        texto = fetch_and_extract_article_text(resolved_url)

        if not texto:
            continue

        if not is_potentially_disaster_related(texto, KEYWORDS):
            print(f"⚠️ Ignorado artigo não relacionado com desastres: {resolved_url}")
            continue

        artigo = processar_item(item, item["keyword"], item["localidade"], texto=texto)
        if artigo:
            batch.append(artigo)

        time.sleep(1)

    # 4️⃣ Guardar todos os artigos relevantes
    if batch:
        guardar_csv_incremental(OUTPUT_CSV, batch)
        total_articles += len(batch)
        print(f"✅ {len(batch)} artigos salvos.")

    # 5️⃣ Atualizar DICOFREG no output final
    if os.path.exists(OUTPUT_CSV):
        try:
            update_dicofreg_column(OUTPUT_CSV, "config/freguesias_com_codigos.json")
            print(f"✅ Coluna DICOFREG atualizada.")
        except Exception as e:
            print(f"❌ Erro ao atualizar DICOFREG: {e}")

    print(f"📊 Total de artigos processados: {total_articles}")


if __name__ == "__main__":
    print("🔧 Starting the Google News scraper...")
    try:
        run_scraper()
        print("✅ Scraper finished successfully.")
    except Exception as e:
        print(f"❌ An error occurred while running the scraper: {e}")

def extract_articles_from_rss(feed_url):
    articles = []
    feed = feedparser.parse(feed_url)
    for entry in feed.entries:
        article = {
            "title": entry.title,
            "link": entry.link,
            "published": entry.published,
            "summary": entry.summary,
        }
        articles.append(article)
    return articles

def resolve_final_url(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.url
    except requests.RequestException:
        return url

def extract_article_content(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        paragraphs = soup.find_all('p')
        content = ' '.join([p.get_text() for p in paragraphs])
        return content
    except requests.RequestException:
        return None
