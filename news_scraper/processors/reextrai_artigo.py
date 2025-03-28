import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from utils.helpers import (
    extract_article_text,
    extract_victim_counts,
    extract_title_from_text,
    limpar_texto_lixo
)

ARQUIVO_API_URL = "https://arquivo.pt/textsearch"
ARQUIVO_VIEW_URL = "https://arquivo.pt/wayback/"

def reextrair_artigo(event_id: str, data: str, fonte: str, termos=[], municipio=""):
    data_obj = datetime.strptime(data, "%Y-%m-%d")
    from_str = data_obj.strftime("%Y%m%d000000")
    to_str = data_obj.strftime("%Y%m%d235959")

    if "publico" in fonte.lower():
        site = "site:publico.pt"
    elif "jn" in fonte.lower():
        site = "site:jn.pt"
    elif "rtp" in fonte.lower():
        site = "site:rtp.pt"
    elif "cm" in fonte.lower():
        site = "site:cmjornal.pt"
    else:
        site = ""

    query = f"{' '.join(termos)} {municipio} {site}".strip()
    print(f"[QUERY] {query}")

    params = {
        "q": query,
        "from": from_str,
        "to": to_str,
        "dedupValue": 2,
        "dedupField": "title",
        "maxItems": 10,
        "offset": 0
    }
    resp = requests.get(ARQUIVO_API_URL, params=params)
    dados = resp.json()
    resultados = dados.get("response_items", [])

    if not resultados:
        print("Nenhum resultado encontrado.")
        return None, ""

    for item in resultados:
        url_original = item.get("linkToArchive")
        print(f"[CANDIDATO] {url_original}")
        html = requests.get(url_original).text
        soup = BeautifulSoup(html, "html.parser")
        artigo = extract_article_text(soup)
        artigo = limpar_texto_lixo(artigo)
        if artigo:
            print("\n[TEXTO DO ARTIGO]:\n")
            print(artigo[:2000], "...\n")

            print("[VÍTIMAS DETETADAS]:")
            print(extract_victim_counts(artigo))

            print("[TÍTULO ESTIMADO]:")
            print(extract_title_from_text(artigo))
            return artigo, artigo
    print("Nenhum artigo útil encontrado.")
    return None, ""

def verificar_csv(caminho_csv: str, limite: int = 10):
    df = pd.read_csv(caminho_csv)
    total = len(df)
    print(f"Verificando até {limite} eventos de {total} no total...\n")

    for idx, row in df.iterrows():
        if idx >= limite:
            break
        id_evt = row["ID"]
        data = row["date"]
        fonte = row["source"]
        municipio = row["municipali"]
        subtype = row["subtype"]
        tipo = row["type"]
        esperadas = {k: row.get(k, 0) for k in ["fatalities", "injured", "evacuated", "displaced", "missing"]}

        print(f"\n➡️ Evento {idx+1}/{limite} | {tipo} - {subtype} em {municipio} ({data})")

        artigo, texto = reextrair_artigo(id_evt, data, fonte, [subtype], municipio)
        if texto:
            print(f"📰 Texto extraído: {texto[:300]}...")
        else:
            print("⚠️ Artigo não encontrado ou texto vazio.")
            continue

        encontrados = extract_victim_counts(texto)
        print("✅ Valores esperados:", esperadas)
        print("🔍 Valores encontrados:", encontrados)

        inconsistencias = {k: (esperadas[k], encontrados[k]) for k in esperadas if esperadas[k] != encontrados[k]}
        if inconsistencias:
            print("❌ Inconsistências:", inconsistencias)
        else:
            print("✔️ Valores consistentes.")

# Exemplo de chamada:
# reextrair_artigo(
#     event_id="a7cab182-e282-4ee2-9926-b97ba13ea508",
#     data="2016-01-03",
#     fonte="Arquivo - publico.pt",
#     termos=["cheia"],
#     municipio="abrantes"
# )

verificar_csv("data/artigos_filtrados.csv", limite=50)
