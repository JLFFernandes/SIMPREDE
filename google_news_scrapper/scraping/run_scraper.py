import os
import csv
import hashlib
import feedparser
import requests
import sys
import time
from datetime import datetime, timedelta
import argparse
import concurrent.futures
import multiprocessing
import asyncio
import aiohttp
import certifi
import ssl
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.helpers import load_keywords, carregar_paroquias_com_municipios, gerar_id
from pathlib import Path

# Updated template to include date parameters
GOOGLE_NEWS_TEMPLATE = "https://news.google.com/rss/search?q={query}+{municipio}+Portugal&hl=pt-PT&gl=PT&ceid=PT:pt&when:24h&before:{date}&after:{date}"
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")
LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")
TODAS_LOCALIDADES = list(LOCALIDADES.keys()) + MUNICIPIOS + DISTRITOS
INTERMEDIATE_CSV = "data/raw/intermediate_google_news.csv"

def format_date(date_str):
    """Convert YYYY-MM-DD date string to Google News date format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        print(f"⚠️ Formato de data inválido: {date_str}. Use YYYY-MM-DD")
        sys.exit(1)

async def fetch_news_for_location_keyword(session, localidade, keyword, target_date):
    query_url = GOOGLE_NEWS_TEMPLATE.format(
        query=keyword.replace(" ", "+"),
        municipio=localidade.replace(" ", "+"),
        date=target_date
    )
    print(f"🌐 URL gerada: {query_url}")
    try:
        async with session.get(query_url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            content = await response.read()
            feed = feedparser.parse(content)
            print(f"🔎 {keyword} em {localidade} ({len(feed.entries)} notícias)")
            artigos = []
            for item in feed.entries:
                pub_date = datetime.strptime(item.published, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d")
                if pub_date == target_date:
                    artigos.append({
                        "ID": gerar_id(item.get("link", "")),
                        "keyword": keyword,
                        "localidade": localidade,
                        "title": item.get("title", ""),
                        "link": item.get("link", ""),
                        "published": item.get("published", "")
                    })
            return artigos
    except Exception as e:
        print(f"❌ Erro em {query_url}: {e}")
        return []

def run_scraper():
    parser = argparse.ArgumentParser(description="Google News Scraper")
    parser.add_argument("--date", help="Data específica para coletar (formato: YYYY-MM-DD)", required=True)
    args = parser.parse_args()

    target_date = format_date(args.date)
    print(f"🔍 Pesquisando notícias para a data: {target_date}")

    async def run_all():
        tasks = []
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(limit=100, ssl=ssl_context)
        async with aiohttp.ClientSession(connector=connector) as session:
            for localidade in TODAS_LOCALIDADES:
                for keyword in KEYWORDS:
                    tasks.append(fetch_news_for_location_keyword(session, localidade, keyword, target_date))
            results = await asyncio.gather(*tasks)

        # Flatten results
        links_coletados = [art for sublist in results for art in sublist]
        save_intermediate_csv(links_coletados)
        print(f"✅ Coleta concluída. Total: {len(links_coletados)} artigos salvos em {INTERMEDIATE_CSV}.")

    asyncio.run(run_all())

def save_intermediate_csv(data):
    if not data:
        return
    os.makedirs(os.path.dirname(INTERMEDIATE_CSV), exist_ok=True)
    with open(INTERMEDIATE_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ID", "keyword", "localidade", "title", "link", "published"])
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    run_scraper()
