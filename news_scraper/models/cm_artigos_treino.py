import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from utils.utils_scraping import extract_article_text
from utils.keywords import weather_terms
from urllib.parse import quote
import time
import os

# Define parâmetros de pesquisa
BASE_API = "https://arquivo.pt/textsearch"
SITE = "correiomanha.pt"
KEYWORDS = weather_terms
DATE_FROM = "2000"
DATE_TO = "2024"
MAX_ITEMS = 100000

# Secções de interesse
noticia_keywords = ["/portugal/", "/mundo/", "/sociedade/", "/desporto/", "/cm-ao-minuto/", "/cultura/", "/opiniao/"]

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
        original = item.get("originalURL", "")
        timestamp = item.get("timestamp", "")
        extracted_text_url = item.get("linkToExtractedText")

        if not title or "desconto" in title.lower() or "oferta" in original:
            continue
        if not any(segment in original for segment in noticia_keywords):
            continue
        if not link:
            continue

        # Extrair texto do artigo
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
df_cm = pd.DataFrame(results)

if not df_cm.empty:
    df_cm["Data"] = pd.to_datetime(df_cm["Data"], errors='coerce')
    df_cm["date"] = df_cm["Data"].dt.date
    df_cm["year"] = df_cm["Data"].dt.year
    df_cm["month"] = df_cm["Data"].dt.month
    df_cm["day"] = df_cm["Data"].dt.day

    # Verifica se o CSV já existe e faz append
    csv_path = "data_temp/raw/publico_arquivo_todos_artigos.csv"
    if os.path.exists(csv_path):
        df_cm.to_csv(csv_path, mode='a', header=False, index=False, encoding="utf-8")
        print("\n✅ Artigos adicionados ao dataset existente.")
    else:
        df_cm.to_csv(csv_path, index=False, encoding="utf-8")
        print("\n✅ CSV criado e artigos guardados.")
else:
    print("⚠️ Nenhum artigo foi guardado.")