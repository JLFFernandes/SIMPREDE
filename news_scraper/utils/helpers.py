import unicodedata
import pandas as pd
import re
from datetime import datetime
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
import json
import csv

# ----------------------------- Normalização -----------------------------

def normalize(text):
    return unicodedata.normalize('NFKD', text.lower()).encode('ASCII', 'ignore').decode('utf-8').strip()

def is_too_similar(a, b, threshold=0.5):
    ratio = SequenceMatcher(None, normalize(a), normalize(b)).ratio()
    return ratio >= threshold

# --------------------------- Extrações de Artigo ---------------------------

def extract_title_from_text(texto: str) -> str:
    linhas = texto.splitlines()
    candidatas = [linha.strip() for linha in linhas if 15 < len(linha.strip()) < 120]
    for linha in candidatas:
        if any(palavra in linha.lower() for palavra in load_keywords("config/keywords.json", idioma="portuguese")):
            return linha
    return candidatas[0] if candidatas else ""

def extract_article_text(soup_or_text) -> str:
    if isinstance(soup_or_text, str):
        lines = soup_or_text.strip().splitlines()
        texto = []
        blacklist = [
            "cookies", "navegacao", "inicie sessao", "assine ja", "facebook", "twitter",
            "login", "publicidade", "versao normal", "comentarios", "registe-se", "palavra-chave",
            "assinatura", "faq", "apoio online", "publico", "menu", "video", "responder", "quiosque"
        ]
        for linha in lines:
            l = linha.strip()
            if len(l) < 30:
                continue
            if any(bl in normalize(l) for bl in blacklist):
                continue
            texto.append(l)
        final_text = " ".join(texto).strip()
        return final_text if len(final_text) > 100 else ""

    containers = [
        soup_or_text.find('article'),
        soup_or_text.find('main'),
        soup_or_text.find('div', class_='article-body'),
        soup_or_text.find('div', class_='story'),
        soup_or_text.find('body')
    ]
    for container in containers:
        if container:
            paragraphs = container.find_all('p')
            texto = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
            final_text = " ".join(texto).strip()
            if final_text:
                return final_text
    paragraphs = soup_or_text.find_all('p')
    texto = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
    return " ".join(texto).strip()

# --------------------------- Extrações Temporais ---------------------------

def parse_event_date(date_str):
    if not date_str:
        return None, None, None, None
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        return date_obj, date_obj.year, date_obj.month, date_obj.day
    except ValueError:
        return None, None, None, None

def extract_event_hour(text: str) -> str | None:
    text = text.lower()
    match = re.search(r'(\d{1,2})h(\d{1,2})?', text)
    if match:
        return match.group(0)
    match_range = re.search(r'entre as (\d{1,2})h.*?e as (\d{1,2})h', text)
    if match_range:
        return f"{match_range.group(1)}h–{match_range.group(2)}h"
    for periodo in ["madrugada", "manha", "tarde", "noite", "inicio da tarde", "fim do dia"]:
        if periodo in text:
            return periodo
    return None

# --------------------------- Extrações de Vítimas ---------------------------

def extract_victim_counts(text: str) -> dict:
    text = text.lower()
    results = {"fatalities": 0, "injured": 0, "evacuated": 0, "displaced": 0, "missing": 0}
    patterns = {
        "fatalities": [
            r"(?P<quant>\d{1,4})\s+(?P<term>mort[oa]s?|faleceram|vitimas?\s+mortais|vitimas?\s+fatais|morreram|morreu|morrer)",
            r"(vitimas?\s+mortais|mortes?)\s+(de\s+)?(?P<quant>\d{1,4})",
            r"(foi|foram)?\s*(?P<quant>\d{1,4})\s+(vitimas?\s+mortais)",
            r"(faleceram|morreram)\s+(?P<quant>\d{1,4})\s+pessoas"
        ],
        "injured": [
            r"(?P<quant>\d{1,4})\s+(?P<term>ferid[oa]s?)",
            r"ficaram\s+(?P<term>ferid[oa]s?)",
            r"registaram-se\s+(?P<quant>\d{1,4})\s+(?P<term>ferid[oa]s?)",
            r"(?P<term>ferid[oa]s?)\s+(ligeiras|graves)"
        ],
        "evacuated": [
            r"(?P<quant>\d{1,4})\s+(?P<term>evacuad[oa]s?)",
            r"(foi|foram)?\s*(evacuad[oa]s?)\s+(?P<quant>\d{1,4})\s+pessoas",
            r"evacua(?:c(?:a|ã)o)\s+(de\s+)?(?P<quant>\d{1,4})",
            r"(?P<quant>\d{1,4})\s+pessoas\s+(retiradas|evacuadas|afastadas)"
        ],
        "displaced": [
            r"(?P<quant>\d{1,4})\s+(?P<term>desalojad[oa]s?)",
            r"(?P<quant>\d{1,4})\s+pessoas\s+(?P<term>sem\s+abrigo|sem\s+casa)",
            r"ficaram\s+(?P<term>sem\s+casa|sem\s+abrigo)",
            r"(foi|foram)?\s*(?P<term>desalojad[oa]s?)\s+(?P<quant>\d{1,4})"
        ],
        "missing": [
            r"(?P<quant>\d{1,4})\s+(?P<term>desaparecid[oa]s?)",
            r"(?P<term>ainda\s+por\s+localizar)",
            r"(?P<term>em\s+parte\s+incerta)",
            r"(continuam|estao|encontram-se)\s+(?P<term>desaparecid[oa]s?)",
            r"(?P<term>nao\s+foi\s+localizado)"
        ]
    }
    for key, regex_list in patterns.items():
        for pattern in regex_list:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    if isinstance(match, tuple):
                        for part in match:
                            if part.isdigit():
                                number = int(part)
                                if number > results[key]:
                                    results[key] = number
                    elif match.isdigit():
                        number = int(match)
                        if number > results[key]:
                            results[key] = number
                except:
                    continue
    return results

# --------------------------- Tipo de desastre ---------------------------

def detect_disaster_type(text: str) -> tuple[str, str]:
    text = normalize(text)
    categorias = {
        "Flood": ["cheia", "inundacao", "alagamento", "transbordo", "inundacoes"],
        "Landslide": ["deslizamento", "desabamento", "desmoronamento", "queda de terra", "movimento de massa"]
    }
    for tipo, termos in categorias.items():
        for termo in termos:
            if termo in text:
                return tipo, termo
    return "Other", "Other"

# --------------------------- Localização ---------------------------

def detect_municipality(text: str, municipios: list[str]) -> str | None:
    text_norm = normalize(text)
    for municipio in municipios:
        if normalize(municipio) in text_norm:
            return municipio
    return None

def is_in_portugal(title: str, article_text: str, localidades: list[str]) -> bool:
    combined = normalize(f"{title} {article_text}")
    return any(normalize(loc) in combined for loc in localidades)

# --------------------------- Carregamento ---------------------------

def load_localidades(caminho_json_municipios: str) -> list[str]:
    with open(caminho_json_municipios, "r", encoding="utf-8") as f:
        localidades = json.load(f)
    return [loc.lower() for loc in localidades]

def load_keywords(caminho_json: str, idioma: str = "portuguese") -> list[str]:
    with open(caminho_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    termos = data.get("weather_terms", {}).get(idioma, [])
    return [normalize(t.strip()) for t in termos if t.strip()]

# --------------------------- Utilitários ---------------------------

def limpar_texto_lixo(texto: str) -> str:
    if not texto:
        return ""
    lixo = [
        "Saltar para o conteudo", "Este site utiliza cookies", "Iniciar sessao",
        "Politica de Privacidade", "Publicidade", "Registe-se", "Assine ja",
        "Versao Normal", "Mais populares", "Ultimas Noticias", "Facebook",
        "Google+", "RSS"
    ]
    if "\n" not in texto:
        return texto.strip()
    linhas = texto.splitlines()
    filtradas = [linha for linha in linhas if not any(lixo_item in normalize(linha) for lixo_item in lixo)]
    if not filtradas:
        return texto.strip()
    return "\n".join(filtradas).strip()

# --------------------------- Escrita de CSV ---------------------------

def guardar_csv(path: str, dados: list[dict]):
    if not dados:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=dados[0].keys())
        writer.writeheader()
        writer.writerows(dados)

# --------------------------- Relevância ---------------------------

def is_potentially_disaster_related(text: str) -> bool:
    print("🎯 Testar relevância para:", text[:150])
    termos = load_keywords("config/keywords.json", idioma="portuguese")
    text_norm = normalize(text)
    print("🔑 Keywords:", termos[:10])
    return any(t in text_norm for t in termos)
