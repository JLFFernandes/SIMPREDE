import pandas as pd
import json
import re
import os
import sys
from datetime import datetime
from typing import Dict, Optional

# Add parent directory to path to allow importing utils
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)  # Add project root to path for imports
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
    url = str(row['page']).lower() if 'page' in row and pd.notna(row['page']) else ""
    distrito = str(row['district']).strip().title() if 'district' in row and pd.notna(row['district']) else ""
    par = str(row['parish']).strip().title() if 'parish' in row and pd.notna(row['parish']) else ""

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

def safe_numeric_conversion(df, columns):
    """Safely convert columns to numeric types"""
    for col in columns:
        if col in df.columns:
            # Convert to numeric with coercion, fill NaN with 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        else:
            # Create column if it doesn't exist
            df[col] = 0
    return df

# Main processing function
def process_articles():
    try:
        # Load the CSV file
        df = pd.read_csv(input_path)
        print(f"Loaded {len(df)} articles from {input_path}")
        print("DataFrame columns:", df.columns.tolist())
        
        # Make sure numeric columns are properly converted
        numeric_columns = ['fatalities', 'injured', 'evacuated', 'displaced', 'missing', 'year']
        print("Column types before conversion:", {col: df[col].dtype for col in df.columns if col in numeric_columns})
        
        # Convert all numeric columns safely
        df = safe_numeric_conversion(df, numeric_columns)
        print("Column types after conversion:", {col: df[col].dtype for col in numeric_columns if col in df.columns})
        
        # Apply initial filtering
        # First create a copy to avoid the SettingWithCopyWarning
        df_vitimas = df.copy()
        
        # Apply nacional filter
        df_vitimas = df_vitimas[df_vitimas.apply(filtra_artigo_nacional, axis=1)]
        print(f"After national filter: {len(df_vitimas)} articles")
        
        # Palavras e domínios que indicam possível irrelevância
        palavras_indesejadas = [
            "brasil", "espanh", "venezuela", "cuba", "nepal", "china", "argentina", "eua", "angola",
            "moçambique", "india", "internacional", "global", "historia", "histórico", "históricas",
            "retrospectiva", "em.com.br", "correiobraziliense", "aviso-amarelo", "aviso-laranja", 
            "previsao", "g1.globo.com", "alerta", "previsto", "emite aviso", "incendios", 
            "desporto", "preve", "avisos", "alertas", "alerta", "aviso", "previsão", "previsões", 
            "previsões meteorológicas", ".com.br", "moçambique", "futuro", "nationalgeographic", 
            "colisao", "belgica"
        ]
        
        # Get current year
        ano_atual = datetime.now().year
        
        # Filter based on URL content if page column exists
        if 'page' in df_vitimas.columns:
            df_vitimas = df_vitimas[~df_vitimas["page"].str.contains("|".join(palavras_indesejadas), 
                                                                     case=False, na=False)]
        
        # Filter by year if year column exists and is numeric
        if 'year' in df_vitimas.columns:
            df_vitimas = df_vitimas[df_vitimas["year"].between(2017, ano_atual)]
        
        print("Applying enhanced rule-based filtering...")
        
        # Relevant keywords for filtering
        palavras_relevantes = ["vitima", "morto", "ferido", "desalojado", "evacuado", "desaparecido", 
                              "tempestade", "inundação", "temporal", "chuva", "vento", "tornado", 
                              "ciclone", "furacão", "deslizamento", "enchente", "tromba", "água"]
        
        # Calculate relevance score
        def calculate_relevance_score(url):
            if not isinstance(url, str):
                return 0
            url = url.lower()
            score = 0
            for palavra in palavras_relevantes:
                if palavra in url:
                    score += 1
            return score
        
        # Add relevance score if page column exists
        if 'page' in df_vitimas.columns:
            df_vitimas['relevance_score'] = df_vitimas['page'].apply(calculate_relevance_score)
        else:
            df_vitimas['relevance_score'] = 0
        
        # Keep articles with relevance or confirmed victims
        df_vitimas = df_vitimas[(df_vitimas['relevance_score'] > 0) | 
                                (df_vitimas['fatalities'] > 0) | 
                                (df_vitimas['injured'] > 0) | 
                                (df_vitimas['evacuated'] > 0) | 
                                (df_vitimas['displaced'] > 0) |
                                (df_vitimas['missing'] > 0)]
        
        print(f"After rule-based filtering: {len(df_vitimas)} articles remain")
        
        # Remove duplicates if page column exists
        if 'page' in df_vitimas.columns:
            print("Removing duplicates based on page URL...")
            df_vitimas = df_vitimas.drop_duplicates(subset=['page'])
        
        # Remove empty rows
        df_vitimas = df_vitimas.dropna(how='all')
        
        # Caminho para o ficheiro JSON de eventos climáticos
        eventos_path = os.path.join(PROJECT_ROOT, "config", "eventos_climaticos.json")
        
        # Carregar eventos climáticos a partir do ficheiro JSON
        with open(eventos_path, 'r', encoding='utf-8') as file:
            eventos_climaticos = json.load(file)
        
        # Identify climate event based on URL
        def identificar_evento(url):
            if isinstance(url, str):
                for e in eventos_climaticos:
                    if e in url.lower():
                        return e
            return None
        
        # Add event name if page column exists
        if 'page' in df_vitimas.columns:
            df_vitimas["evento_nome"] = df_vitimas["page"].apply(identificar_evento)
            df_vitimas["evento_nome"] = df_vitimas["evento_nome"].fillna("desconhecido")
        else:
            df_vitimas["evento_nome"] = "desconhecido"
        
        # Remove duplicates based on event, date and impact
        columns_to_check = ["evento_nome", "date", "fatalities", "injured", "displaced"]
        existing_columns = [col for col in columns_to_check if col in df_vitimas.columns]
        
        if existing_columns:
            print(f"Removing duplicates based on columns: {existing_columns}")
            df_vitimas = df_vitimas.drop_duplicates(subset=existing_columns)
        else:
            print("Warning: No common columns found for removing duplicates based on event details")
            df_vitimas = df_vitimas.drop_duplicates()
        
        print("Skipping content validation and victim count updates at this stage...")
        
        # Create final result
        df_filtered = df_vitimas.copy()
        
        # Convert DataFrame to list of dictionaries
        def df_to_dict_list(df):
            return df.to_dict(orient='records')
        
        # Import helper functions
        from utils.helpers import guardar_csv_incremental, organize_path_by_date
        
        # Save paths
        raw_filtered_path = os.path.join(raw_data_dir, f"artigos_filtrados_{current_date}.csv")
        structured_filtered_path = os.path.join(structured_dir, f"artigos_filtrados_{current_date}.csv")
        
        # Convert DataFrame to list of dictionaries for saving
        articles_dict_list = df_to_dict_list(df_filtered)
        
        # Only save if there's actual data to save
        if len(articles_dict_list) > 0:
            print(f"Found {len(articles_dict_list)} articles to save")
            
            # Save to structured directory using helper function which will handle year/month/day organization
            print(f"Saving filtered articles to: {structured_filtered_path}")
            guardar_csv_incremental(structured_filtered_path, articles_dict_list)
            
            print(f"✅ Articles saved successfully")
        else:
            print("⚠️ No articles to save after filtering. Skipping file creation.")
        
        print(f"Processing completed successfully")
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Run the process
    process_articles()
