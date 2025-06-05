import sys
import os
import logging
import argparse
import pandas as pd
import json
import re
from datetime import datetime, timedelta

# Set unbuffered output at the very beginning - FIXED VERSION
os.environ['PYTHONUNBUFFERED'] = '1'
# Force stdout to be unbuffered using reconfigure (Python 3.7+)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(line_buffering=True)

# Add the missing imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.helpers import guardar_csv_incremental

# Configure logging for Airflow compatibility with immediate flushing
def setup_airflow_logging():
    """Setup logging that works well with Airflow UI"""
    import logging
    
    class ImmediateFlushHandler(logging.StreamHandler):
        def emit(self, record):
            super().emit(record)
            self.flush()
            # Also flush the underlying stream
            if hasattr(self.stream, 'flush'):
                self.stream.flush()
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger("filtrar_artigos_vitimas_airflow")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    handler = ImmediateFlushHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    
    return logger

# Initialize logger
logger = setup_airflow_logging()

def log_progress(message, level="info", flush=True):
    """Log with guaranteed immediate visibility for Airflow"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"[{timestamp}] {message}"
    
    # Always print to stdout with immediate flush
    print(formatted_message, flush=True)
    
    # Also use logger for proper log levels
    if level == "warning":
        logger.warning(message)
    elif level == "error":
        logger.error(message)
    elif level == "debug":
        logger.debug(message)
    else:
        logger.info(message)
    
    # Force all flushes - safe version
    if hasattr(sys.stdout, 'flush'):
        sys.stdout.flush()
    if hasattr(sys.stderr, 'flush'):
        sys.stderr.flush()

def log_statistics(stats_dict, title="Statistics"):
    """Log statistics in a formatted way for better readability"""
    log_progress(f"=" * 50)
    log_progress(f"{title}")
    log_progress(f"=" * 50)
    for key, value in stats_dict.items():
        log_progress(f"  {key}: {value}")
    log_progress(f"=" * 50)

def find_input_file(project_root, target_date):
    """
    Find the input CSV file from processar_relevantes_airflow output,
    checking the year/month/day organized structure
    """
    current_date = target_date.strftime("%Y%m%d")
    current_year = target_date.strftime("%Y")
    current_month = target_date.strftime("%m")
    current_day = target_date.strftime("%d")
    date_suffix = target_date.strftime("%Y-%m-%d")  # Format used by processar_relevantes
    
    # Look for the output from processar_relevantes_airflow
    possible_paths = [
        # Date-specific organized structure from processar_relevantes_airflow (raw data)
        os.path.join(project_root, "data", "raw", current_year, current_month, current_day, f"artigos_google_municipios_pt_{date_suffix}.csv"),
        # Date-specific organized structure from processar_relevantes_airflow (structured data)
        os.path.join(project_root, "data", "structured", current_year, current_month, current_day, f"artigos_google_municipios_pt_{date_suffix}.csv"),
        # Legacy format with date suffix
        os.path.join(project_root, "data", "raw", current_year, current_month, current_day, f"artigos_google_municipios_pt_{current_date}.csv"),
        # Fallback to structured directory with date suffix
        os.path.join(project_root, "data", "structured", f"artigos_google_municipios_pt_{date_suffix}.csv"),
        # Fallback to structured directory
        os.path.join(project_root, "data", "structured", "artigos_google_municipios_pt.csv"),
        # Fallback to raw directory
        os.path.join(project_root, "data", "raw", f"artigos_google_municipios_pt_{current_date}.csv")
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            log_progress(f"✅ Found input file: {path}")
            return path
    
    log_progress(f"❌ No input file found. Searched in:", "error")
    for path in possible_paths:
        log_progress(f"   - {path}", "error")
    
    return None

def setup_paths_and_dates(target_date=None, dias=1):
    """Setup all paths and dates for filtering"""
    if target_date:
        if isinstance(target_date, str):
            dt = datetime.strptime(target_date, "%Y-%m-%d")
        else:
            dt = target_date
    else:
        dt = datetime.now() - timedelta(days=dias-1)
    
    current_date = dt.strftime("%Y%m%d")
    current_year = dt.strftime("%Y")
    current_month = dt.strftime("%m")
    current_day = dt.strftime("%d")
    date_suffix = dt.strftime("%Y-%m-%d")  # Format used by processar_relevantes
    
    # Get the directory where this script is located
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # Get the project root directory (go up from processador -> google_scraper -> scripts)
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
    
    log_progress(f"🔍 Path setup:")
    log_progress(f"   Script dir: {SCRIPT_DIR}")
    log_progress(f"   Project root: {PROJECT_ROOT}")
    
    # Create directory structure for structured data by year/month/day
    STRUCTURED_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "structured", current_year, current_month, current_day)
    os.makedirs(STRUCTURED_DATA_DIR, exist_ok=True)
    
    # Find input file using the search function
    INPUT_CSV = find_input_file(PROJECT_ROOT, dt)
    
    # Define output file paths with consistent naming
    OUTPUT_CSV = os.path.join(STRUCTURED_DATA_DIR, f"artigos_vitimas_filtrados_{date_suffix}.csv")
    
    # Standard output filename for backward compatibility
    DEFAULT_OUTPUT_CSV = os.path.join(PROJECT_ROOT, "data", "structured", "artigos_vitimas_filtrados.csv")
    
    return {
        'input_csv': INPUT_CSV,
        'output_csv': OUTPUT_CSV,
        'default_output_csv': DEFAULT_OUTPUT_CSV,
        'structured_data_dir': STRUCTURED_DATA_DIR,
        'project_root': PROJECT_ROOT,
        'current_date': current_date,
        'dt': dt
    }

def has_victims(article):
    """Check if an article has any victim counts"""
    victim_fields = ["fatalities", "injured", "evacuated", "displaced", "missing"]
    return any(article.get(field, 0) > 0 for field in victim_fields)

def filter_articles_with_victims(df):
    """Filter articles that have at least one victim count > 0"""
    victim_columns = ["fatalities", "injured", "evacuated", "displaced", "missing"]
    
    # Ensure victim columns exist and are numeric
    for col in victim_columns:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Filter rows where at least one victim count is > 0
    victim_mask = df[victim_columns].sum(axis=1) > 0
    filtered_df = df[victim_mask].copy()
    
    return filtered_df

def guardar_csv_incremental_with_date(output_csv, default_output_csv, artigos):
    """
    Save to date-specific file in the structured data directory with year/month/day structure
    and also maintain backward compatibility with structured directory
    """
    # Save to structured data directory (this will be organized by year/month/day)
    guardar_csv_incremental(output_csv, artigos)
    
    # Also save to standard file for backward compatibility
    guardar_csv_incremental(default_output_csv, artigos)
    
    log_progress(f"✅ Filtered files saved with year/month/day organization")

def load_config_data(project_root):
    """Load municipios and eventos climaticos configuration"""
    # Try multiple paths for config files (same as processar_relevantes_airflow.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    config_paths = [
        os.path.join(project_root, "config"),
        os.path.join(script_dir, "config"),
        os.path.join(os.path.dirname(script_dir), "config"),
        "config"
    ]
    
    log_progress(f"🔍 Looking for config files in multiple paths:")
    for path in config_paths:
        log_progress(f"   Checking: {path}")
    
    municipios_data = None
    eventos_climaticos = None
    
    # Try each config path until we find the files
    for config_path in config_paths:
        municipios_path = os.path.join(config_path, "municipios_por_distrito.json")
        eventos_path = os.path.join(config_path, "eventos_climaticos.json")
        
        if os.path.exists(municipios_path) and os.path.exists(eventos_path):
            log_progress(f"✅ Found config files in: {config_path}")
            log_progress(f"   Municipios: {municipios_path}")
            log_progress(f"   Eventos: {eventos_path}")
            
            try:
                with open(municipios_path, 'r', encoding='utf-8') as file:
                    municipios_data = json.load(file)
                log_progress(f"✅ Loaded municipios config with {len(municipios_data)} districts")
                
                with open(eventos_path, 'r', encoding='utf-8') as file:
                    eventos_climaticos = json.load(file)
                log_progress(f"✅ Loaded eventos config with {len(eventos_climaticos)} events")
                
                break
                
            except Exception as e:
                log_progress(f"❌ Error loading config files from {config_path}: {e}", "error")
                continue
    
    # Check if we successfully loaded the configs
    if municipios_data is None:
        log_progress(f"❌ Municipios config file not found in any location", "error")
        raise FileNotFoundError(f"Municipios config file not found in any of the searched paths")
    
    if eventos_climaticos is None:
        log_progress(f"❌ Eventos config file not found in any location", "error")
        raise FileNotFoundError(f"Eventos config file not found in any of the searched paths")
    
    # Extract valid districts and parishes
    distritos_validos = set(municipios_data.keys())
    paroquias_validas = {par for municipios in municipios_data.values() for par in municipios}
    
    log_progress(f"📊 Config summary: {len(distritos_validos)} districts, {len(paroquias_validas)} parishes")
    
    return distritos_validos, paroquias_validas, eventos_climaticos

def filtra_artigo_nacional(row, distritos_validos, paroquias_validas):
    """Filter for national articles with valid districts and parishes"""
    # International exclusion keywords
    palavras_excluidas = [
        "espanha", "franca", "nepal", "cuba", "eua", "brasil", "japao", "valencia",
        "internacional", "mundo", "europa", "america", "africa", "global", "china", "paquistao"
    ]
    
    url = str(row['page']).lower() if 'page' in row and pd.notna(row['page']) else ""
    distrito = str(row['district']).strip().title() if 'district' in row and pd.notna(row['district']) else ""
    par = str(row['parish']).strip().title() if 'parish' in row and pd.notna(row['parish']) else ""

    # Check if district is valid
    if not distrito or distrito not in distritos_validos:
        return False

    # Check if parish is valid
    if par and par not in paroquias_validas:
        return False

    # Check for excluded words in URL
    if any(palavra in url for palavra in palavras_excluidas):
        return False

    return True

def safe_numeric_conversion(df, columns):
    """Safely convert columns to numeric types"""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        else:
            df[col] = 0
    return df

def calculate_relevance_score(url, palavras_relevantes):
    """Calculate relevance score based on keywords"""
    if not isinstance(url, str):
        return 0
    url = url.lower()
    score = 0
    for palavra in palavras_relevantes:
        if palavra in url:
            score += 1
    return score

def identificar_evento(url, eventos_climaticos):
    """Identify climate event based on URL"""
    if isinstance(url, str):
        for e in eventos_climaticos:
            if e in url.lower():
                return e
    return "desconhecido"

def apply_comprehensive_filters(df, project_root):
    """Apply all filtering logic from the original script"""
    log_progress("🔧 Loading configuration data...")
    distritos_validos, paroquias_validas, eventos_climaticos = load_config_data(project_root)
    
    log_progress(f"📊 Starting comprehensive filtering with {len(df)} articles")
    
    # Make sure numeric columns are properly converted
    numeric_columns = ['fatalities', 'injured', 'evacuated', 'displaced', 'missing', 'year']
    df = safe_numeric_conversion(df, numeric_columns)
    
    # Apply national filter
    log_progress("🇵🇹 Applying national article filter...")
    initial_count = len(df)
    df = df[df.apply(lambda row: filtra_artigo_nacional(row, distritos_validos, paroquias_validas), axis=1)]
    log_progress(f"   After national filter: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Unwanted keywords filter
    log_progress("🚫 Applying unwanted keywords filter...")
    palavras_indesejadas = [
        "brasil", "espanh", "venezuela", "cuba", "nepal", "china", "argentina", "eua", "angola",
        "moçambique", "india", "internacional", "global", "historia", "histórico", "históricas",
        "retrospectiva", "em.com.br", "correiobraziliense", "aviso-amarelo", "aviso-laranja", 
        "previsao", "g1.globo.com", "alerta", "previsto", "emite aviso", "incendios", 
        "desporto", "preve", "avisos", "alertas", "alerta", "aviso", "previsão", "previsões", 
        "previsões meteorológicas", ".com.br", "moçambique", "futuro", "nationalgeographic", 
        "colisao", "belgica"
    ]
    
    initial_count = len(df)
    if 'page' in df.columns:
        df = df[~df["page"].str.contains("|".join(palavras_indesejadas), case=False, na=False)]
    log_progress(f"   After unwanted keywords filter: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Year filter
    log_progress("📅 Applying year range filter...")
    ano_atual = datetime.now().year
    initial_count = len(df)
    if 'year' in df.columns:
        df = df[df["year"].between(2017, ano_atual)]
    log_progress(f"   After year filter: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Relevance score calculation
    log_progress("⭐ Calculating relevance scores...")
    palavras_relevantes = ["vitima", "morto", "ferido", "desalojado", "evacuado", "desaparecido", 
                          "tempestade", "inundação", "temporal", "chuva", "vento", "tornado", 
                          "ciclone", "furacão", "deslizamento", "enchente", "tromba", "água"]
    
    if 'page' in df.columns:
        df['relevance_score'] = df['page'].apply(lambda url: calculate_relevance_score(url, palavras_relevantes))
    else:
        df['relevance_score'] = 0
    
    # Keep articles with relevance or confirmed victims
    log_progress("🎯 Applying relevance and victim filters...")
    initial_count = len(df)
    df = df[(df['relevance_score'] > 0) | 
            (df['fatalities'] > 0) | 
            (df['injured'] > 0) | 
            (df['evacuated'] > 0) | 
            (df['displaced'] > 0) |
            (df['missing'] > 0)]
    log_progress(f"   After relevance/victim filter: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Remove duplicates based on page URL
    if 'page' in df.columns:
        log_progress("🔄 Removing URL duplicates...")
        initial_count = len(df)
        df = df.drop_duplicates(subset=['page'])
        log_progress(f"   After URL deduplication: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Remove empty rows
    df = df.dropna(how='all')
    
    # Add event identification
    log_progress("🌦️ Identifying climate events...")
    if 'page' in df.columns:
        df["evento_nome"] = df["page"].apply(lambda url: identificar_evento(url, eventos_climaticos))
        df["evento_nome"] = df["evento_nome"].fillna("desconhecido")
    else:
        df["evento_nome"] = "desconhecido"
    
    # Remove duplicates based on event, date and impact
    log_progress("🔄 Removing event-based duplicates...")
    columns_to_check = ["evento_nome", "date", "fatalities", "injured", "displaced"]
    existing_columns = [col for col in columns_to_check if col in df.columns]
    
    if existing_columns:
        initial_count = len(df)
        df = df.drop_duplicates(subset=existing_columns)
        log_progress(f"   After event deduplication: {len(df)} articles (removed {initial_count - len(df)})")
    
    return df

def airflow_main(target_date=None, dias=1):
    """
    Main function optimized for Airflow execution
    Returns number of articles with victims for Airflow compatibility
    """
    log_progress("🚀 Starting filtrar_artigos_vitimas_airflow")
    
    # Setup paths and configuration
    try:
        paths = setup_paths_and_dates(target_date, dias)
        if not paths['input_csv']:
            log_progress("❌ No input file found. Cannot proceed.", "error")
            return 0
        
    except Exception as e:
        log_progress(f"❌ Setup failed: {e}", "error")
        raise
    
    try:
        # Load the processed articles
        log_progress(f"📂 Loading articles from: {paths['input_csv']}")
        df = pd.read_csv(paths['input_csv'])
        
        initial_stats = {
            "Total Articles Loaded": len(df),
            "Input File": paths['input_csv'],
            "Output File": paths['output_csv'],
            "Days Filter": dias
        }
        log_statistics(initial_stats, "Filtering Started")
        
        # Apply comprehensive filtering (same as original script)
        log_progress("🔍 Applying comprehensive filtering logic...")
        filtered_df = apply_comprehensive_filters(df, paths['project_root'])
        
        if filtered_df.empty:
            log_progress("⚠️ No articles remaining after comprehensive filtering.", "warning")
            return 0
        
        # Convert DataFrame to list of dictionaries for compatibility
        artigos_filtrados = filtered_df.to_dict('records')
        
        # Calculate comprehensive statistics
        comprehensive_stats = {
            "Articles After All Filters": len(artigos_filtrados),
            "Articles with Victims": len(filtered_df[(filtered_df['fatalities'] > 0) | 
                                                   (filtered_df['injured'] > 0) | 
                                                   (filtered_df['evacuated'] > 0) | 
                                                   (filtered_df['displaced'] > 0) |
                                                   (filtered_df['missing'] > 0)]),
            "Total Fatalities": filtered_df['fatalities'].sum(),
            "Total Injured": filtered_df['injured'].sum(),
            "Total Evacuated": filtered_df['evacuated'].sum(),
            "Total Displaced": filtered_df['displaced'].sum(),
            "Total Missing": filtered_df['missing'].sum(),
            "Average Relevance Score": f"{filtered_df['relevance_score'].mean():.2f}" if 'relevance_score' in filtered_df.columns else "N/A"
        }
        log_statistics(comprehensive_stats, "Comprehensive Filtering Results")
        
        # Save filtered articles
        log_progress("💾 Saving comprehensively filtered articles...")
        guardar_csv_incremental_with_date(paths['output_csv'], paths['default_output_csv'], artigos_filtrados)
        
        final_stats = {
            "Total Articles Filtered": len(artigos_filtrados),
            "Filter Rate": f"{len(artigos_filtrados)/len(df)*100:.1f}%" if len(df) > 0 else "0%",
            "Output Files": f"{paths['output_csv']} and {paths['default_output_csv']}"
        }
        log_statistics(final_stats, "Comprehensive Filtering Completed Successfully")
        
        return len(artigos_filtrados)
        
    except FileNotFoundError as e:
        log_progress(f"❌ File not found error: {str(e)}", "error")
        return 0
    except Exception as e:
        log_progress(f"❌ Error during filtering: {str(e)}", "error")
        import traceback
        log_progress(f"❌ Full traceback: {traceback.format_exc()}", "error")
        raise

def main():
    """Main function with argument parsing for CLI usage"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Filter articles with victims (Airflow version)")
    parser.add_argument("--dias", type=int, default=1, help="Number of days to process")
    parser.add_argument("--date", type=str, help="Specific target date (YYYY-MM-DD)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Configure logging level
    if args.debug:
        logger.setLevel(logging.DEBUG)
        log_progress("🔍 DEBUG logging enabled", "debug")
    
    log_progress("Starting filtrar_artigos_vitimas_airflow")
    log_progress(f"Parameters: dias={args.dias}, date={args.date}")
    
    try:
        result = airflow_main(target_date=args.date, dias=args.dias)
        log_progress(f"✅ Filtering completed. Found {result} articles with victims")
        return 0
    except Exception as e:
        log_progress(f"❌ Filtering failed: {e}", "error")
        return 1

if __name__ == "__main__":
    sys.exit(main())
