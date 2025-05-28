import pandas as pd
import json  # Importar para carregar o JSON
import re
import joblib
from datetime import datetime
from joblib import load
import requests
from bs4 import BeautifulSoup
from typing import Dict, Optional, Tuple
import os

# Get current date for filename
current_date = datetime.now().strftime("%Y%m%d")
current_year = datetime.now().strftime("%Y")
current_month = datetime.now().strftime("%m")
current_day = datetime.now().strftime("%d")

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root directory (one level up)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Create path for raw data by year/month/day
raw_data_dir = os.path.join(PROJECT_ROOT, "data", "raw", current_year, current_month, current_day)
structured_dir = os.path.join(PROJECT_ROOT, "data", "structured")

# Ensure directories exist
os.makedirs(raw_data_dir, exist_ok=True)
os.makedirs(structured_dir, exist_ok=True)

# Check if there's a date-specific file in year/month/day structure
raw_year_month_day_path = os.path.join(
    raw_data_dir, f"artigos_google_municipios_pt_{current_date}.csv"
)
structured_date_path = os.path.join(
    structured_dir, f"artigos_google_municipios_pt_{current_date}.csv"
)
default_path = os.path.join(
    structured_dir, "artigos_google_municipios_pt.csv"
)

# Choose input path based on file existence
if os.path.exists(raw_year_month_day_path):
    input_path = raw_year_month_day_path
    print(f"Using raw year/month/day file: {input_path}")
elif os.path.exists(structured_date_path):
    input_path = structured_date_path
    print(f"Using structured date file: {input_path}")
else:
    input_path = default_path
    print(f"Using default file: {input_path}")

# Define output paths
output_path = os.path.join(raw_data_dir, f"artigos_publico_{current_date}.csv")
filtered_csv = os.path.join(raw_data_dir, f"artigos_filtrados_{current_date}.csv")
base_output_path = os.path.join(structured_dir, "artigos_filtrados_{}.csv")
municipios_path = os.path.join(PROJECT_ROOT, "config", "municipios_por_distrito.json")

# Carregar distritos e paróquias válidas do JSON
with open(municipios_path, 'r', encoding='utf-8') as file:
    municipios_data = json.load(file)

# Extrair distritos válidos
distritos_validos = set(municipios_data.keys())

# Extrair paróquias válidas
paroquias_validas = {par for municipios in municipios_data.values() for par in municipios}

# Indicadores de artigos internacionais
palavras_excluidas = [
    "espanha", "franca", "nepal", "cuba", "eua", "brasil", "japao", "valencia",
    "internacional", "mundo", "europa", "america", "africa", "global", "china", "paquistao"
]

def filtra_artigo_nacional(row):
    url = str(row['page']).lower()
    distrito = str(row['district']).strip().title()
    par = str(row['parish']).strip().title()

    # Verificar se o distrito é válido
    if not distrito or distrito not in distritos_validos:
        return False

    # Verificar se a paróquia é válida
    if par and par not in paroquias_validas:
        return False

    # Verificar palavras excluídas na URL
    if any(palavra in url for palavra in palavras_excluidas):
        return False

    return True

def extract_article_content(url: str) -> Optional[str]:
    """
    Extracts the main content from a news article URL
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9"
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove unwanted elements
        for tag in ['script', 'style', 'nav', 'header', 'footer', 'iframe', 'aside']:
            for element in soup.find_all(tag):
                element.decompose()
        
        # Try different content selectors
        for selector in ['article', '.article-body', '.content-text', 'main', '#content-core']:
            content = soup.select_one(selector)
            if content:
                # Get all paragraphs
                paragraphs = content.find_all('p')
                if paragraphs:
                    text = ' '.join(p.get_text(strip=True) for p in paragraphs)
                    if len(text) > 100:  # Minimum content length
                        return text
        
        return None
    except Exception as e:
        print(f"Error extracting content from {url}: {str(e)}")
        return None

def extract_victim_counts(text: str) -> Dict[str, int]:
    """
    Extracts victim counts from article text using regex patterns
    """
    patterns = {
        'fatalities': r'(?:morto|mortos|fatalidade|vítima mortal|óbito)[^\d]*(\d+)',
        'injured': r'(?:ferido|feridos|hospitalizado|internado)[^\d]*(\d+)',
        'evacuated': r'(?:evacuad[oa]s?|retirad[oa]s?)[^\d]*(\d+)',
        'displaced': r'(?:desalojad[oa]s?|deslocad[oa]s?)[^\d]*(\d+)',
        'missing': r'(?:desaparecid[oa]s?)[^\d]*(\d+)'
    }
    
    counts = {}
    for category, pattern in patterns.items():
        matches = re.findall(pattern, text, re.IGNORECASE)
        counts[category] = max([int(n) for n in matches] + [0])
    
    return counts

# This function will not be used in this stage
def validate_article(row: pd.Series) -> bool:
    """
    Validates article content and updates victim counts if necessary
    """
    return True  # Simply return True as we're not doing validation at this stage

# Carregar CSV
df = pd.read_csv(input_path)

# Aplicar filtro de vítimas (keeping this initial filter based on existing data)
df_vitimas = df[
    (df['fatalities'] >= 0) |
    (df['injured'] >= 0) |
    (df['evacuated'] >= 0) |
    (df['displaced'] >= 0) |
    (df['missing'] >= 0)
]

# Aplicar filtro nacional
df_vitimas = df_vitimas[df_vitimas.apply(filtra_artigo_nacional, axis=1)]

# Palavras e domínios que indicam possível irrelevância
palavras_indesejadas = [
    "brasil", "espanh", "venezuela", "cuba", "nepal", "china", "argentina", "eua", "angola",
    "moçambique", "india", "internacional", "global", "historia", "histórico", "históricas",
    "retrospectiva", "em.com.br", "correiobraziliense", "aviso-amarelo", "aviso-laranja", "previsao", "g1.globo.com", "alerta", "previsto", "emite aviso", "incendios", 
    "desporto", "preve", "avisos", "alertas", "alerta", "aviso", "previsão", "previsões", "previsões meteorológicas", ".com.br", "moçambique", "futuro", "nationalgeographic", "colisao", "belgica"
]

# Ano atual
ano_atual = datetime.now().year

# Filtro baseado no conteúdo da URL
df_vitimas = df_vitimas[~df_vitimas["page"].str.contains("|".join(palavras_indesejadas), case=False, na=False)]

# Filtro de datas antigas e futuras irreais
df_vitimas = df_vitimas[df_vitimas["year"].between(2017, ano_atual)]

# Enhanced rule-based filtering (replacing ML approach)
print("Applying enhanced rule-based filtering...")

# Additional filtering based on title keywords
palavras_relevantes = ["vitima", "morto", "ferido", "desalojado", "evacuado", "desaparecido", 
                      "tempestade", "inundação", "temporal", "chuva", "vento", "tornado", 
                      "ciclone", "furacão", "deslizamento", "enchente", "tromba", "água"]

# Create a relevance score based on presence of relevant keywords
def calculate_relevance_score(url):
    if not isinstance(url, str):
        return 0
    url = url.lower()
    score = 0
    for palavra in palavras_relevantes:
        if palavra in url:
            score += 1
    return score

# Add relevance score to dataframe
df_vitimas['relevance_score'] = df_vitimas['page'].apply(calculate_relevance_score)

# Keep articles with some relevance or confirmed victims
df_vitimas = df_vitimas[(df_vitimas['relevance_score'] > 0) | 
                        (df_vitimas['fatalities'] > 0) | 
                        (df_vitimas['injured'] > 0) | 
                        (df_vitimas['evacuated'] > 0) | 
                        (df_vitimas['displaced'] > 0) |
                        (df_vitimas['missing'] > 0)]

print(f"After rule-based filtering: {len(df_vitimas)} articles remain")

# Eliminar duplicados
df_vitimas.drop_duplicates(subset='page', inplace=True)

# Remover linhas em branco
df_vitimas.dropna(how='all', inplace=True)

# Caminho para o ficheiro JSON de eventos climáticos
eventos_path = os.path.join(PROJECT_ROOT, "config", "eventos_climaticos.json")

# Carregar eventos climáticos a partir do ficheiro JSON
with open(eventos_path, 'r', encoding='utf-8') as file:
    eventos_climaticos = json.load(file)

def identificar_evento(url):
    if isinstance(url, str):
        for e in eventos_climaticos:
            if e in url.lower():
                return e
    return None

df_vitimas["evento_nome"] = df_vitimas["page"].apply(identificar_evento)

# Preencher eventos não identificados
df_vitimas["evento_nome"] = df_vitimas["evento_nome"].fillna("desconhecido")

# Remover duplicados com base em evento climático, data e impacto
columns_to_check = ["evento_nome", "date", "fatalities", "injured", "displaced"]
existing_columns = [col for col in columns_to_check if col in df_vitimas.columns]
if existing_columns:
    df_vitimas = df_vitimas.drop_duplicates(subset=existing_columns, keep="first")
else:
    df_vitimas = df_vitimas.drop_duplicates(keep="first")

# Modified section - Skip validation of articles and updating victim counts
print("Skipping content validation and victim count updates at this stage...")

# Create a copy of the filtered dataframe as our final result
df_filtered = df_vitimas.copy()

# Convert DataFrame to list of dictionaries for guardar_csv_incremental
def df_to_dict_list(df):
    return df.to_dict(orient='records')

# Import our organize_path_by_date function
from utils.helpers import guardar_csv_incremental, organize_path_by_date

# Guardar todos os artigos num CSV principal
raw_filtered_path = os.path.join(raw_data_dir, f"artigos_filtrados_{current_date}.csv")
structured_filtered_path = os.path.join(structured_dir, f"artigos_filtrados_{current_date}.csv")
structured_default_path = os.path.join(structured_dir, "artigos_filtrados.csv")

# Save to both locations using our helper function
print(f"Saving to raw data directory: {raw_filtered_path}")
df_filtered.to_csv(raw_filtered_path, index=False)

# Save to structured directory with year/month/day organization
articles_dict_list = df_to_dict_list(df_filtered)
guardar_csv_incremental(structured_filtered_path, articles_dict_list)
guardar_csv_incremental(structured_default_path, articles_dict_list)

# Guardar artigos separados por fonte
fontes = ["jn", "publico", "cnnportugal", "sicnoticias"]

for fonte in fontes:
    df_fonte = df_filtered[df_filtered["page"].str.contains(fonte, case=False, na=False)]
    if not df_fonte.empty:
        # Save with date in filename in both raw and structured directories
        raw_fonte_path = os.path.join(raw_data_dir, f"artigos_{fonte}_{current_date}.csv")
        structured_fonte_path = os.path.join(structured_dir, f"artigos_{fonte}_{current_date}.csv")
        structured_fonte_default = os.path.join(structured_dir, f"artigos_{fonte}.csv")
        
        # Save to raw data directory
        df_fonte.to_csv(raw_fonte_path, index=False)
        
        # Save to structured directory with year/month/day organization using helpers
        fonte_dict_list = df_to_dict_list(df_fonte)
        guardar_csv_incremental(structured_fonte_path, fonte_dict_list)
        guardar_csv_incremental(structured_fonte_default, fonte_dict_list)

print(f"✅ Files saved in raw data directory: {raw_data_dir}")
print(f"✅ Files saved in structured directory: {structured_dir}")
print(f"Articles divided by source: {[f'artigos_{fonte}_{current_date}.csv' for fonte in fontes]}")
