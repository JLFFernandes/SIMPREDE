import spacy
import pandas as pd
import os
import re
import sys
import requests
from bs4 import BeautifulSoup

MODEL_PATH = os.environ.get("VICTIM_NER_MODEL_PATH", "../models/victims_nlp")
MODEL_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), MODEL_PATH))
INPUT_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/artigos_google_municipios_pt.csv"))
OUTPUT_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/ner_victims_list.csv"))

PT_NUMBER_WORDS = {
    "um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "quatro": 4, "cinco": 5, "seis": 6, "sete": 7,
    "oito": 8, "nove": 9, "dez": 10, "onze": 11, "doze": 12, "treze": 13, "catorze": 14, "quatorze": 14,
    "quinze": 15, "dezasseis": 16, "dezesseis": 16, "dezessete": 17, "dezanove": 19, "dezoito": 18, "dezanove": 19,
    "vinte": 20, "trinta": 30, "quarenta": 40, "cinquenta": 50, "sessenta": 60, "setenta": 70, "oitenta": 80, "noventa": 90,
    "cem": 100, "cento": 100, "duzentos": 200, "trezentos": 300, "quatrocentos": 400, "quinhentos": 500,
    "seiscentos": 600, "setecentos": 700, "oitocentos": 800, "novecentos": 900, "mil": 1000
}

def fetch_and_extract_article_text(url):
    try:
        resp = requests.get(url, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        paragraphs = [p.get_text() for p in soup.find_all("p")]
        return "\n".join(paragraphs).strip()
    except Exception as e:
        print(f"⚠️ Erro ao extrair texto de {url}: {e}")
        return ""

def extract_number_from_text(text):
    match = re.search(r"\d+", text)
    if match:
        return int(match.group())
    words = re.findall(r"\w+", text.lower())
    for word in words:
        if word in PT_NUMBER_WORDS:
            return PT_NUMBER_WORDS[word]
    return 1

def extract_mentions(article_row, ner):
    mentions = []
    texto = article_row.get("texto", None)
    if not texto or texto == "" or pd.isna(texto):
        url = article_row.get("page", "")
        texto = fetch_and_extract_article_text(url)
    if not texto:
        return mentions
    doc = ner(texto)
    for ent in doc.ents:
        if ent.label_ in {"FATALITIES", "INJURED", "EVACUATED", "DISPLACED", "MISSING"}:
            sent = ent.sent.text if hasattr(ent, "sent") else ""
            value = extract_number_from_text(ent.text)
            mention = {
                "ID": article_row.get("ID"),
                "date": article_row.get("date"),
                "type": article_row.get("type"),
                "subtype": article_row.get("subtype"),
                "district": article_row.get("district"),
                "municipali": article_row.get("municipali"),
                "parish": article_row.get("parish"),
                "page": article_row.get("page"),
                "entity_label": ent.label_,
                "entity_text": ent.text,
                "entity_value": value,
                "sentence": sent
            }
            mentions.append(mention)
    return mentions

def main():
    print(f"🔎 Carregando modelo NER de vítimas de: {MODEL_PATH}")
    try:
        ner = spacy.load(MODEL_PATH)
        # Ensure sentence boundaries are set
        if not ner.has_pipe("senter") and not ner.has_pipe("parser") and not ner.has_pipe("sentencizer"):
            ner.add_pipe("sentencizer")
    except Exception as e:
        print(f"❌ Não foi possível carregar o modelo: {e}")
        return

    df = pd.read_csv(INPUT_CSV)
    total = len(df)
    print(f"📄 Total de artigos a processar: {total}")

    all_mentions = []
    for idx, row in df.iterrows():
        article_id = row.get("ID", "")
        url = row.get("page", "")
        print(f"➡️ [{idx+1}/{total}] Processando artigo ID={article_id} URL={url}")
        try:
            mentions = extract_mentions(row, ner)
            print(f"   ↳ {len(mentions)} menções extraídas.")
            all_mentions.extend(mentions)
        except Exception as e:
            print(f"   ⚠️ Erro ao processar artigo ID={article_id}: {e}")

    if not all_mentions:
        print("⚠️ Nenhuma menção de vítima extraída.")
        return

    mentions_df = pd.DataFrame(all_mentions)
    mentions_df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Lista de vítimas extraídas (NER) salva em: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
