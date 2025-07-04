"""
Normalizador e extrator de informações de textos relacionados a desastres naturais
Preparado para produção com otimizações de performance
"""

#TOFO: Comentarios de código em português e preparado para produção
# File: airflow-GOOGLE-NEWS-SCRAPER/scripts/google_scraper/extracao/normalizador.py
# Script para normalizar e extrair informações de textos relacionados a desastres naturais

import re
from datetime import datetime
import unicodedata
import json
from urllib.parse import urlparse
import pandas as pd

# Mapeamento de palavras para números
NUM_PALAVRAS = {
    "uma": 1, "um": 1, "duas": 2, "dois": 2, "tres": 3, "quatro": 4, "cinco": 5, "seis": 6,
    "sete": 7, "oito": 8, "nove": 9, "dez": 10, "onze": 11, "doze": 12, "treze": 13, "catorze": 14,
    "quatorze": 14, "quinze": 15, "dezasseis": 16, "dezesseis": 16, "dezanove": 19, "dezoito": 18,
    "vinte": 20
}

def normalize(text):
    """Normalize text for comparison - handles None values"""
    if text is None:
        return ""
    return unicodedata.normalize('NFKD', str(text).lower()).encode('ASCII', 'ignore').decode('utf-8').strip()

def palavras_para_numeros(text):
    for palavra, numero in NUM_PALAVRAS.items():
        text = re.sub(rf"\b{palavra}\b", str(numero), text)
    return text

def extract_victim_counts(text: str) -> dict:
    text = palavras_para_numeros(normalize(text))
    results = {"fatalities": 0, "injured": 0, "evacuated": 0, "displaced": 0, "missing": 0}
    patterns = {
        "fatalities": [r"(\d+)\s+(pessoas?\s+mortas?|vitimas?\s+mortais?|falecid[oa]s?|morreram)"],
        "injured": [r"(\d+)\s+(pessoas?\s+feridas?|ferid[oa]s?)"],
        "evacuated": [r"(\d+)\s+(pessoas?\s+evacuadas?|evacuad[oa]s?)"],
        "displaced": [r"(\d+)\s+(pessoas?\s+desalojadas?|desalojad[oa]s?|deslocad[oa]s?)"],
        "missing": [r"(\d+)\s+(pessoas?\s+desaparecidas?|desaparecid[oa]s?)"]
    }
    for key, regexes in patterns.items():
        for pattern in regexes:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                num = int(match.group(1))
                if num < 1000:  # realistic upper bound
                    results[key] = num
                break
    return results

def inferir_titulo_do_url(url: str) -> str:
    try:
        path = urlparse(url).path
        slug = path.split("/")[-1]
        slug = re.sub(r"[-_]+", " ", slug)
        slug = re.sub(r"\.(html|htm|php)$", "", slug)
        slug = re.sub(r"[^\w\s]", "", slug)
        return slug.strip()
    except:
        return ""

def extrair_vitimas(titulo: str, corpo: str, url: str = None) -> dict:
    titulo_norm = normalize(titulo) if titulo else ""
    corpo_norm = normalize(corpo) if corpo else ""

    vitimas_titulo = extract_victim_counts(titulo_norm)
    if any(vitimas_titulo.values()):
        return vitimas_titulo

    if url:
        titulo_url = inferir_titulo_do_url(url)
        vitimas_url = extract_victim_counts(titulo_url)
        if any(vitimas_url.values()):
            return vitimas_url

    return extract_victim_counts(corpo_norm)

def limpar_texto_lixo(texto: str) -> str:
    if not texto:
        return ""

    repetitive_patterns = [
        r"we usecookiesand data to.*?if you choose to “accept all,”",
        r"we will also use cookies and data to.*?if you"
    ]
    for pattern in repetitive_patterns:
        texto = re.sub(pattern, "", texto, flags=re.IGNORECASE).strip()

    lixo = [
        "Saltar para o conteudo", "Este site utiliza cookies", "Iniciar sessao",
        "Politica de Privacidade", "Publicidade", "Registe-se", "Assine ja",
        "Versao Normal", "Mais populares", "Ultimas Noticias", "Facebook",
        "Google+", "RSS"
    ]
    linhas = texto.splitlines()
    filtradas = [linha for linha in linhas if not any(lixo_item in normalize(linha) for lixo_item in lixo)]
    return "\n".join(filtradas).strip()


def extract_title_from_text(texto: str) -> str:
    linhas = texto.splitlines()
    candidatas = [linha.strip() for linha in linhas if 15 < len(linha.strip()) < 120]
    return candidatas[0] if candidatas else ""


def extract_event_hour(text: str) -> str | None:
    text = text.lower()
    match = re.search(r'(\d{1,2})h(\d{1,2})?', text)
    if match:
        return match.group(0)
    for periodo in ["madrugada", "manha", "tarde", "noite"]:
        if periodo in text:
            return periodo
    return None


def parse_event_date(date_str):
    """
    Analisa e converte string de data para formato estruturado
    Retorna tuple com (date_obj, year, month, day)
    """
    if not date_str or pd.isna(date_str):
        return None, None, None, None
    
    try:
        # Lista de formatos de data suportados
        formatos_data = [
            "%Y-%m-%d",
            "%d/%m/%Y", 
            "%d-%m-%Y",
            "%Y/%m/%d",
            "%d.%m.%Y",
            "%Y-%m-%d %H:%M:%S"
        ]
        
        date_str_clean = str(date_str).strip()
        
        for formato in formatos_data:
            try:
                date_obj = datetime.strptime(date_str_clean, formato).date()
                return date_obj, date_obj.year, date_obj.month, date_obj.day
            except ValueError:
                continue
        
        # Se nenhum formato funcionar, tenta parse automático
        try:
            import dateutil.parser
            date_obj = dateutil.parser.parse(date_str_clean).date()
            return date_obj, date_obj.year, date_obj.month, date_obj.day
        except:
            pass
            
    except Exception:
        pass
    
    return None, None, None, None

def detect_disaster_type(text: str) -> tuple[str, str]:
    """
    Detecta tipo de desastre natural baseado no texto
    Retorna tuple com (tipo_principal, subtipo)
    """
    if not text:
        return "Other", "Other"
    
    text_normalizado = normalize(text)
    
    # Mapeamento otimizado de categorias de desastres
    categorias_desastres = {
        "Flood": {
            "termos": ["cheia", "inundacao", "alagamento", "transbordo", "enchente"],
            "subtipos": {
                "flash_flood": ["cheia rapida", "torrente"],
                "river_flood": ["rio", "margem", "leito"],
                "urban_flood": ["cidade", "urbano", "rua"]
            }
        },
        "Landslide": {
            "termos": ["deslizamento", "desabamento", "desmoronamento", "queda de terra"],
            "subtipos": {
                "rockfall": ["pedra", "rocha"],
                "mudslide": ["lama", "terra"],
                "debris_flow": ["detrito", "material"]
            }
        },
        "Fire": {
            "termos": ["incendio", "fogo", "chama", "queimada"],
            "subtipos": {
                "wildfire": ["florestal", "mata", "campo"],
                "urban_fire": ["edificio", "casa", "predio"],
                "industrial_fire": ["fabrica", "industria"]
            }
        },
        "Storm": {
            "termos": ["tempestade", "temporal", "vento", "vendaval", "ciclone"],
            "subtipos": {
                "windstorm": ["vento", "vendaval"],
                "hailstorm": ["granizo", "saraiva"],
                "thunderstorm": ["trovoada", "relampago"]
            }
        }
    }
    
    # Procura correspondências
    for tipo_principal, dados in categorias_desastres.items():
        # Verifica termos principais
        for termo in dados["termos"]:
            if termo in text_normalizado:
                # Procura subtipo específico
                for subtipo, termos_subtipo in dados["subtipos"].items():
                    for termo_subtipo in termos_subtipo:
                        if termo_subtipo in text_normalizado:
                            return tipo_principal, subtipo
                
                # Se não encontrar subtipo específico, usa o termo encontrado
                return tipo_principal, termo
    
    return "Other", "Other"


def verificar_localizacao(texto, MAPA_LOCALIZACOES):
    texto_norm = normalize(texto)
    for item in MAPA_LOCALIZACOES:
        mun = normalize(item["municipio"])
        if mun in texto_norm:
            return {
                "municipali": item["municipio"].upper(),
                "district": item["distrito"].upper(),
                "parish": None,
                "dicofreg": None,
                "georef": "location based on text scraping"
            }
    return None

def is_potentially_disaster_related(text: str, keywords: list[str]) -> bool:
    text = normalize(text)
    return any(keyword in text for keyword in keywords)