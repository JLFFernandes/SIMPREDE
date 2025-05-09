from spacy.tokens import DocBin
from spacy.training import Example
import spacy
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import time
import os
from urllib.parse import urlparse
import re
from extracao.extractor import resolve_google_news_url, fetch_and_extract_article_text
from utils.helpers import carregar_paroquias_com_municipios, load_keywords, carregar_dicofreg, guardar_csv_incremental, detect_municipality 
from extracao.normalizador import detect_disaster_type, normalize, is_potentially_disaster_related
from datetime import datetime
import hashlib
from concurrent.futures import ThreadPoolExecutor

INPUT_CSV = "data/intermediate_google_news_relevantes.csv"
OUTPUT_CSV = "data/artigos_google_municipios_pt.csv"
LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")
FREGUESIAS_COM_CODIGOS = carregar_dicofreg()
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")

# Load your fine-tuned NER model if available
NER_MODEL_PATH = os.environ.get("VICTIM_NER_MODEL_PATH", "").strip()
if not NER_MODEL_PATH:
    # Always resolve relative to this script's directory
    NER_MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "models", "victims_nlp")
    NER_MODEL_PATH = os.path.abspath(NER_MODEL_PATH)
print(f"🔎 Tentando carregar modelo NER de vítimas de: {NER_MODEL_PATH}")
ner = None
if NER_MODEL_PATH:
    try:
        ner = spacy.load(NER_MODEL_PATH)
        # Ensure sentence boundaries are set
        if not ner.has_pipe("senter") and not ner.has_pipe("parser") and not ner.has_pipe("sentencizer"):
            ner.add_pipe("sentencizer")
    except Exception as e:
        print(f"⚠️ Não foi possível carregar o modelo NER de vítimas: {e}")
        ner = None
else:
    print("⚠️ Caminho do modelo NER de vítimas não definido. Extração de vítimas será ignorada.")

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


# Helper to convert Portuguese number words to digits
PT_NUMBER_WORDS = {
    "um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "quatro": 4, "cinco": 5, "seis": 6, "sete": 7,
    "oito": 8, "nove": 9, "dez": 10, "onze": 11, "doze": 12, "treze": 13, "catorze": 14, "quatorze": 14,
    "quinze": 15, "dezasseis": 16, "dezesseis": 16, "dezessete": 17, "dezanove": 19, "dezoito": 18, "dezanove": 19,
    "vinte": 20, "trinta": 30, "quarenta": 40, "cinquenta": 50, "sessenta": 60, "setenta": 70, "oitenta": 80, "noventa": 90,
    "cem": 100, "cento": 100, "duzentos": 200, "trezentos": 300, "quatrocentos": 400, "quinhentos": 500,
    "seiscentos": 600, "setecentos": 700, "oitocentos": 800, "novecentos": 900, "mil": 1000
}

def extract_number_from_text(text):
    # Try to find a digit first
    match = re.search(r"\d+", text)
    if match:
        return int(match.group())
    # Try to find a number word
    words = re.findall(r"\w+", text.lower())
    for word in words:
        if word in PT_NUMBER_WORDS:
            return PT_NUMBER_WORDS[word]
    return 1  # fallback

def extract_victims_ner(text):
    if ner is None:
        return {"fatalities": 0, "injured": 0, "evacuated": 0, "displaced": 0, "missing": 0}
    
    result = {"fatalities": 0, "injured": 0, "evacuated": 0, "displaced": 0, "missing": 0}
    sentences = [sent.text for sent in ner(text).sents if re.search(r"(mort[oa]s?|ferid[oa]s?|evacuad[oa]s?|desalojad[oa]s?|desaparecid[oa]s?)", sent.text, re.IGNORECASE)]
    
    for sentence in sentences:
        doc = ner(sentence)
        for ent in doc.ents:
            label = ent.label_
            value = extract_number_from_text(ent.text)
            if value >= 1000:  # unrealistic number, skip
                continue
            if label == "FATALITIES":
                result["fatalities"] += value
            elif label == "INJURED":
                result["injured"] += value
            elif label == "EVACUATED":
                result["evacuated"] += value
            elif label == "DISPLACED":
                result["displaced"] += value
            elif label == "MISSING":
                result["missing"] += value
    return result

def extract_main_body(text, title=None):
    """
    Extracts the main body of the article, removing the title if it appears at the start.
    """
    if title and text.startswith(title):
        # Remove the title from the beginning if present
        return text[len(title):].lstrip(" :\n\r-")
    return text

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

    # Only use the main body for NER extraction
    main_body = extract_main_body(texto, titulo)

    tipo, subtipo = detect_disaster_type(texto)
    # Primeiro tenta extrair vítimas do título; se não encontrar nenhuma, tenta no corpo principal
    vitimas = extract_victims_ner(titulo)
    if all(v == 0 for v in vitimas.values()):
        vitimas = extract_victims_ner(main_body)
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
        "missing": vitimas["missing"],
    }

def carregar_links_existentes(output_csv):
    if not os.path.exists(output_csv):
        return set()
    try:
        df_existente = pd.read_csv(output_csv)
        return set(df_existente["page"].dropna().unique())
    except Exception:
        return set()

SAVE_EVERY = 10  # Save every 10 articles (change as needed)

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
    artigos_a_processar = relevantes.iloc[start_index:].to_dict('records')
    total = len(artigos_a_processar)
    with ThreadPoolExecutor(max_workers=4) as executor:
        for idx, artigo in enumerate(executor.map(processar_artigo, artigos_a_processar), 1):
            if artigo:
                artigos_final.append(artigo)
            if len(artigos_final) % SAVE_EVERY == 0:
                guardar_csv_incremental(OUTPUT_CSV, artigos_final)
                print(f"💾 {len(artigos_final)} artigos salvos até agora...")
        # Save any remaining articles not yet saved
        if len(artigos_final) % SAVE_EVERY != 0:
            guardar_csv_incremental(OUTPUT_CSV, artigos_final)
            print(f"💾 {len(artigos_final)} artigos salvos (final).")

    if artigos_final:
        print(f"✅ Base de dados final atualizada com {len(artigos_final)} artigos.")
    else:
        print("⚠️ Nenhum artigo foi processado com sucesso.")


def criar_docbin_exemplo():
    nlp = spacy.blank("pt")
    db = DocBin()

    exemplos = [
        ("Duas pessoas morreram na enchente.", "Duas pessoas morreram", "FATALITIES"),
        ("Três pessoas morreram.", "Três pessoas", "FATALITIES"),
        ("Uma vítima mortal foi registada.", "Uma vítima mortal", "FATALITIES"),
        ("Morreram cinco pessoas.", "Morreram cinco pessoas", "FATALITIES"),
        ("O desastre causou quatro mortes.", "quatro mortes", "FATALITIES"),
        ("Foram confirmadas dez mortes.", "dez mortes", "FATALITIES"),
        ("Houve uma vítima mortal.", "uma vítima mortal", "FATALITIES"),
        ("Sete pessoas perderam a vida.", "Sete pessoas", "FATALITIES"),
        ("O acidente resultou em duas mortes.", "duas mortes", "FATALITIES"),
        ("Cinco ficaram feridas após o desabamento.", "Cinco ficaram feridas", "INJURED"),
        ("Sete pessoas ficaram feridas.", "Sete pessoas ficaram feridas", "INJURED"),
        ("Houve três feridos.", "três feridos", "INJURED"),
        ("O acidente deixou dez feridos.", "dez feridos", "INJURED"),
        ("Quatro pessoas ficaram feridas.", "Quatro pessoas ficaram feridas", "INJURED"),
        ("Foram registados dois feridos.", "dois feridos", "INJURED"),
        ("Vinte pessoas sofreram ferimentos.", "Vinte pessoas sofreram ferimentos", "INJURED"),
        ("O deslizamento provocou oito feridos.", "oito feridos", "INJURED"),
        ("Vinte pessoas foram evacuadas devido à inundação.", "Vinte pessoas foram evacuadas", "EVACUATED"),
        ("Foram evacuadas dez famílias.", "dez famílias", "EVACUATED"),
        ("Cerca de 100 pessoas evacuadas.", "100 pessoas evacuadas", "EVACUATED"),
        ("Mais de cinquenta pessoas evacuadas.", "cinquenta pessoas evacuadas", "EVACUATED"),
        ("Sessenta pessoas foram retiradas das casas.", "Sessenta pessoas foram retiradas", "EVACUATED"),
        ("Cerca de 30 pessoas ficaram sem casa.", "30 pessoas ficaram sem casa", "DISPLACED"),
        ("O temporal deixou vinte desalojados.", "vinte desalojados", "DISPLACED"),
        ("Mais de cem pessoas ficaram desalojadas.", "cem pessoas ficaram desalojadas", "DISPLACED"),
        ("Uma pessoa está desaparecida.", "Uma pessoa está desaparecida", "MISSING"),
        ("Três pessoas estão desaparecidas.", "Três pessoas estão desaparecidas", "MISSING"),
        ("Há relatos de cinco desaparecidos.", "cinco desaparecidos", "MISSING"),
        ("Ainda falta localizar uma pessoa.", "uma pessoa", "MISSING"),
        ("Quatro moradores estão em paradeiro desconhecido.", "Quatro moradores estão em paradeiro desconhecido", "MISSING"),
    ]

    for text, span_text, label in exemplos:
        doc = nlp.make_doc(text)
        start = text.find(span_text)
        end = start + len(span_text)
        span = doc.char_span(start, end, label=label, alignment_mode="contract")
        if span is None:
            print(f"⚠️ Erro de alinhamento em: {text} [{start}:{end}] → '{span_text}'")
            continue
        doc.ents = [span]
        db.add(doc)

    db.to_disk(os.path.join(os.path.dirname(__file__), "..", "models", "train_data.spacy"))
    print("✅ Dados de treino salvos em 'train_data.spacy'")


if __name__ == "__main__":
    main()

# Note: To create the training data, run criar_docbin_exemplo() once to generate 'train_data.spacy'.
# Then train your model using spaCy's CLI, e.g.:
# python -m spacy train config.cfg --paths.train train_data.spacy --paths.dev dev_data.spacy


