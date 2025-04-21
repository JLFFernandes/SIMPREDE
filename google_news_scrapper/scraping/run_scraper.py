import os
import csv
import hashlib
import feedparser
import sys
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.helpers import load_keywords, carregar_paroquias_com_municipios, gerar_id
from pathlib import Path

GOOGLE_NEWS_TEMPLATE = "https://news.google.com/rss/search?q={query}+{municipio}+Portugal&hl=pt-PT&gl=PT&ceid=PT:pt"
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")
LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")
TODAS_LOCALIDADES = list(LOCALIDADES.keys()) + MUNICIPIOS + DISTRITOS
INTERMEDIATE_CSV = "data/intermediate_google_news.csv"


def run_scraper():
    print(f"🔍 A pesquisar combinações de {len(KEYWORDS)} palavras com {len(TODAS_LOCALIDADES)} localidades...")

    links_coletados = []
    for i, localidade in enumerate(TODAS_LOCALIDADES, 1):
        for keyword in KEYWORDS:
            query_url = GOOGLE_NEWS_TEMPLATE.format(
                query=keyword.replace(" ", "+"),
                municipio=localidade.replace(" ", "+")
            )
            feed = feedparser.parse(query_url)
            print(f"🔎 {keyword} em {localidade} ({len(feed.entries)} notícias)")

            for item in feed.entries:
                links_coletados.append({
                    "ID": gerar_id(item.get("link", "")),
                    "keyword": keyword,
                    "localidade": localidade,
                    "title": item.get("title", ""),
                    "link": item.get("link", ""),
                    "published": item.get("published", "")
                })

        if i % 10 == 0:
            print(f"💾 Salvando após {i} localidades...")
            save_intermediate_csv(links_coletados)

    save_intermediate_csv(links_coletados)
    print(f"✅ Coleta concluída. Total: {len(links_coletados)} artigos salvos em {INTERMEDIATE_CSV}.")

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
