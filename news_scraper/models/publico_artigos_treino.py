import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from utils.utils_scraping import (
    extract_article_text, load_localidades
)
from utils.keywords import weather_terms
from urllib.parse import quote
import time

# Carregar lista de localidades portuguesas (opcional, pode ser útil mais tarde)
localidades = load_localidades("utils/municipios.json")

# Define parametros de pesquisa
BASE_API = "https://arquivo.pt/textsearch"
SITE = "publico.pt"
KEYWORDS = weather_terms
DATE_FROM = "1990"
DATE_TO = "2024"
MAX_ITEMS = 100000

results = []

for keyword in KEYWORDS:
    query = f"{quote(keyword)} site:{SITE}"
    url = f"{BASE_API}?q={query}&from={DATE_FROM}&to={DATE_TO}&maxItems={MAX_ITEMS}"

    print(f"\n🔍 Consultando: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Erro na API: {response.status_code}")
        continue

    data = response.json()
    items = data.get("response_items", [])

    print(f"📦 {len(items)} resultados encontrados para '{keyword}'")

    for item in items:
        title = item.get("title", "")
        link = item.get("linkToArchive")
        original = item.get("originalURL")
        timestamp = item.get("timestamp", "")
        extracted_text_url = item.get("linkToExtractedText")

        if not link:
            continue

        article_text = ""
        if extracted_text_url:
            try:
                text_response = requests.get(extracted_text_url)
                if text_response.status_code == 200:
                    article_text = text_response.text
            except:
                pass

        if not article_text:
            try:
                page = requests.get(link, headers={"User-Agent": "Mozilla/5.0"})
                if page.status_code == 200:
                    soup = BeautifulSoup(page.text, 'html.parser')
                    article_text = extract_article_text(soup)
            except:
                continue

        results.append({
            "Título": title,
            "Link": original,
            "Data": timestamp,
            "Texto do Artigo": article_text
        })

        print(f"✅ Artigo guardado: {title[:60]}...")
        time.sleep(0.5)

# Criar DataFrame
df = pd.DataFrame(results)

# Converter timestamp
if not df.empty:
    df["Data"] = pd.to_datetime(df["Data"], errors='coerce')
    df["date"] = df["Data"].dt.date
    df["year"] = df["Data"].dt.year
    df["month"] = df["Data"].dt.month
    df["day"] = df["Data"].dt.day

    df.to_csv("data_temp/raw/publico_arquivo_todos_artigos.csv", index=False, encoding="utf-8")
    print("\n✅ Concluído: todos os artigos guardados em 'publico_arquivo_todos_artigos.csv'")
else:
    print("⚠️ Nenhum artigo foi guardado.")
