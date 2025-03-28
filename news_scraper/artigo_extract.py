import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

# === CONFIGURAÇÕES ===
CSV_INPUT = "data/artigos_filtrados.csv"
CSV_OUTPUT = "newsapi_completo_com_texto.csv"
COLUNA_LINK = "link_extraido"
COLUNA_TEXTO = "fulltext"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def extrair_texto_da_pagina(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            paragrafos = soup.find_all("p")
            texto = " ".join(p.get_text() for p in paragrafos).strip()

            # Fallback: If no text is found in <p> tags, try extracting from <div> or <article>
            if not texto:
                divs = soup.find_all("div")
                texto = " ".join(div.get_text() for div in divs).strip()
            if not texto:
                article = soup.find("article")
                texto = article.get_text().strip() if article else ""

            if texto:
                return texto
            else:
                print(f"⚠️ Nenhum texto extraído de: {url}")
                return ""
        else:
            print(f"❌ Erro {response.status_code} em: {url}")
            return ""
    except Exception as e:
        print(f"⚠️ Erro ao aceder {url}: {e}")
        return ""


def main():
    df = pd.read_csv(CSV_INPUT)

    if COLUNA_LINK not in df.columns:
        print(f"❌ A coluna '{COLUNA_LINK}' não foi encontrada no CSV.")
        return

    textos_extraidos = []
    total = len(df)

    for i, url in enumerate(df[COLUNA_LINK]):
        print(f"🔎 {i+1}/{total} - A processar: {url}")
        texto = extrair_texto_da_pagina(url)
        textos_extraidos.append(texto)
        time.sleep(1)  # evitar bloqueios

    df[COLUNA_TEXTO] = textos_extraidos
    df.to_csv(CSV_OUTPUT, index=False)
    print(f"\n✅ Ficheiro com texto guardado como: {CSV_OUTPUT}")


if __name__ == "__main__":
    main()
