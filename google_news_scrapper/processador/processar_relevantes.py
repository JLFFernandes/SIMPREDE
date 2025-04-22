import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import time
import os
from urllib.parse import urlparse
import re
from extracao.extractor import resolve_google_news_url, fetch_and_extract_article_text
from utils.helpers import carregar_paroquias_com_municipios, load_keywords, carregar_dicofreg, guardar_csv_incremental, detect_municipality, gerar_id
from extracao.normalizador import detect_disaster_type, extract_victim_counts, parse_event_date, normalize, is_potentially_disaster_related
from datetime import datetime
import hashlib

INPUT_CSV = "data/intermediate_google_news_relevantes.csv"
OUTPUT_CSV = "data/artigos_google_municipios_pt.csv"
LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")
FREGUESIAS_COM_CODIGOS = carregar_dicofreg()
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")


def extrair_nome_fonte(url):
    """
    Extrai o nome da fonte (ex: 'publico', 'observador') a partir do domínio do URL.
    """
    try:
        netloc = urlparse(url).netloc
        base = netloc.replace("www.", "").split(".")[0]
        # Remove números, hífens e outras anomalias
        base = re.sub(r'[^a-zA-Z]', '', base)
        return base.lower()
    except:
        return "desconhecido"

def formatar_data_para_ddmmyyyy(published_raw):
    """
    Transforma uma data tipo 'Fri, 24 Jan 2025 07:00:00 GMT' em:
    - '24/01/2025'
    - ano: 2025
    - mes: 1
    - dia: 24
    - hora: 07:00
    """
    try:
        data = pd.to_datetime(published_raw, errors='coerce')
        if pd.isnull(data):
            return "", None, None, None, ""
        return data.strftime("%d/%m/%Y"), data.year, data.month, data.day, data.strftime("%H:%M")
    except Exception:
        return "", None, None, None, ""

def processar_artigo(row):
    original_url = row["link"]
    titulo = row["title"]
    localidade = row["localidade"]
    keyword = row.get("keyword", "desconhecido")
    publicado = row.get("published", "")

    resolved_url = resolve_google_news_url(original_url)
    if not resolved_url or not resolved_url.startswith("http"):
        print(f"⚠️ Link não resolvido: {original_url}")
        return None

    texto = fetch_and_extract_article_text(resolved_url)
    if not texto or not is_potentially_disaster_related(texto, KEYWORDS):
        print(f"⚠️ Artigo ignorado após extração: {titulo}")
        return None

    tipo, subtipo = detect_disaster_type(texto)
    vitimas = extract_victim_counts(texto)
    loc = detect_municipality(texto, LOCALIDADES) or localidade
    district = LOCALIDADES.get(loc.lower(), {}).get("district", "")
    concelho = LOCALIDADES.get(loc.lower(), {}).get("municipality", "")
    parish_normalized = normalize(loc.lower())
    dicofreg = FREGUESIAS_COM_CODIGOS.get(parish_normalized, "")

    data_evt_formatada, ano, mes, dia, hora_evt = formatar_data_para_ddmmyyyy(publicado)
    fonte = extrair_nome_fonte(resolved_url)

    article_id = row["ID"]
    if not article_id:
        return None

    return {
        "ID": article_id,
        "type": tipo,
        "subtype": subtipo,
        "date": data_evt_formatada,
        "year": ano,
        "month": mes,
        "day": dia,
        "hour": hora_evt,
        "georef": loc,
        "district": district,
        "municipali": concelho,
        "parish": loc,
        "DICOFREG": dicofreg,
        "source": fonte,
        "sourcedate": datetime.today().date().isoformat(),
        "sourcetype": "web",
        "page": resolved_url,
        "fatalities": vitimas["fatalities"],
        "injured": vitimas["injured"],
        "evacuated": vitimas["evacuated"],
        "displaced": vitimas["displaced"],
        "missing": vitimas["missing"]
    }

def carregar_links_existentes(output_csv):
    if not os.path.exists(output_csv):
        return set()
    try:
        df_existente = pd.read_csv(output_csv)
        return set(df_existente["page"].dropna().unique())
    except Exception:
        return set()

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Erro: O arquivo de entrada '{INPUT_CSV}' não foi encontrado.")
        return

    # Carregar links já importados
    links_existentes = carregar_links_existentes(OUTPUT_CSV)

    df = pd.read_csv(INPUT_CSV)
    relevantes = df[df["label"] == 1]
    print(f"📊 Total de artigos relevantes a processar: {len(relevantes)}")
    # Tentar identificar o último ID processado
    df_existente = pd.read_csv(OUTPUT_CSV) if os.path.exists(OUTPUT_CSV) else pd.DataFrame()
    ultimo_id = df_existente["ID"].dropna().iloc[-1] if not df_existente.empty else None
 
    # Obter a posição do último ID no DataFrame de relevantes
    start_index = 0
    if ultimo_id:
        try:
            start_index = relevantes[relevantes["ID"] == ultimo_id].index[-1] + 1
            print(f"⏩ A retomar do índice {start_index} após o ID: {ultimo_id}")
        except IndexError:
            print("⚠️ Último ID não encontrado em relevantes. Começando do início.")
 
    artigos_final = []
    for i, row in enumerate(relevantes.iloc[start_index:].iterrows(), start=start_index + 1):
        original_url = row[1]["link"]

        # Verificar se o link já foi importado ANTES de resolver o URL
        if original_url in links_existentes:
            print(f"⏩ Link já importado, pulando: {original_url}")
            continue

        resolved_url = resolve_google_news_url(original_url)
        if resolved_url in links_existentes:
            print(f"⏩ Link resolvido já importado, pulando: {resolved_url}")
            continue

        artigo = processar_artigo(row[1])
        if artigo:
            artigos_final.append(artigo)
            links_existentes.add(artigo["page"])
            print(f"✅ Artigo processado: {artigo['ID']}")

        if i % 10 == 0:
            guardar_csv_incremental(OUTPUT_CSV, artigos_final)
            artigos_final = []

        time.sleep(1)

    if artigos_final:
        guardar_csv_incremental(OUTPUT_CSV, artigos_final)
        print(f"✅ Base de dados final atualizada com {len(artigos_final)} artigos.")
    else:
        print("⚠️ Nenhum artigo foi processado com sucesso.")

if __name__ == "__main__":
    main()