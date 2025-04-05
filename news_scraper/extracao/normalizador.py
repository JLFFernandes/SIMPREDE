import re
from datetime import datetime
import unicodedata

def normalize(text):
    return unicodedata.normalize('NFKD', text.lower()).encode('ASCII', 'ignore').decode('utf-8').strip()

def limpar_texto_lixo(texto: str) -> str:
    if not texto:
        return ""

    # Remove repetitive beginning text using regex for variations
    repetitive_patterns = [
        r"we usecookiesand data to.*?if you choose to “accept all,”",  # Match variations of the repetitive text
        r"we will also use cookies and data to.*?if you"  # Additional pattern for variations
    ]
    for pattern in repetitive_patterns:
        texto = re.sub(pattern, "", texto, flags=re.IGNORECASE).strip()

    # Debugging: Print the cleaned text for verification
    print(f"🔍 Texto após limpeza inicial: {texto[:100]}...")

    # Remove other predefined "lixo"
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
    if not date_str:
        return None, None, None, None
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        return date_obj, date_obj.year, date_obj.month, date_obj.day
    except ValueError:
        return None, None, None, None



def detect_disaster_type(text: str) -> tuple[str, str]:
    text = normalize(text)
    categorias = {
        "Flood": ["cheia", "inundacao", "alagamento", "transbordo"],
        "Landslide": ["deslizamento", "desabamento", "desmoronamento", "queda de terra"]
    }
    for tipo, termos in categorias.items():
        for termo in termos:
            if termo in text:
                return tipo, termo
    return "Other", "Other"

def extract_victim_counts(text: str) -> dict:
    text = text.lower()
    results = {"fatalities": 0, "injured": 0, "evacuated": 0, "displaced": 0, "missing": 0}
    patterns = {
        "fatalities": [r"(\d+)\s+(mort[oa]s?|faleceram|morreram)"],
        "injured": [r"(\d+)\s+(ferid[oa]s?)"],
        "evacuated": [r"(\d+)\s+(evacuad[oa]s?)"],
        "displaced": [r"(\d+)\s+(desalojad[oa]s?)"],
        "missing": [r"(\d+)\s+(desaparecid[oa]s?)"]
    }
    for key, regexes in patterns.items():
        for pattern in regexes:
            match = re.search(pattern, text)
            if match:
                results[key] = int(match.group(1))
    return results

def verificar_localizacao(texto):
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
    # Accept all texts as potentially disaster-related
    return True
