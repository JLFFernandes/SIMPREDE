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

def setup_paths_and_dates(target_date=None, dias=1, input_file=None, output_dir=None, date_str=None):
    """Setup all paths and dates for filtering with controlled output paths"""
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
    date_suffix = date_str if date_str else dt.strftime("%Y-%m-%d")  # Use provided date_str or default format
    
    # Get the directory where this script is located
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # Get the project root directory (go up from processador -> google_scraper -> scripts)
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
    
    log_progress(f"🔍 Path setup:")
    log_progress(f"   Script dir: {SCRIPT_DIR}")
    log_progress(f"   Project root: {PROJECT_ROOT}")
    
    # Use controlled output directory if provided
    if output_dir:
        PROCESSED_DATA_DIR = output_dir
        log_progress(f"📁 Using controlled output directory: {PROCESSED_DATA_DIR}")
    else:
        # Fallback to default structure
        PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed", current_year, current_month, current_day)
    
    # Ensure directories exist
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    # Use provided input file or search for it
    if input_file and os.path.exists(input_file):
        INPUT_CSV = input_file
        log_progress(f"📁 Using provided input file: {INPUT_CSV}")
    else:
        # Find input file using the search function
        INPUT_CSV = find_input_file(PROJECT_ROOT, dt)
        if not INPUT_CSV and input_file:
            log_progress(f"⚠️ Provided input file not found: {input_file}", "warning")
    
    # Define output file paths with controlled naming
    OUTPUT_CSV = os.path.join(PROCESSED_DATA_DIR, f"artigos_vitimas_filtrados_{date_suffix}.csv")
    
    # Additional output files for better organization
    NO_VICTIMS_CSV = os.path.join(PROCESSED_DATA_DIR, f"artigos_sem_vitimas_{date_suffix}.csv")
    FILTERING_LOG = os.path.join(PROCESSED_DATA_DIR, f"filtering_log_{current_date}.log")
    STATS_JSON = os.path.join(PROCESSED_DATA_DIR, f"filtering_stats_{current_date}.json")
    
    # Standard output filename for backward compatibility
    DEFAULT_OUTPUT_CSV = os.path.join(PROJECT_ROOT, "data", "structured", "artigos_vitimas_filtrados.csv")
    
    return {
        'input_csv': INPUT_CSV,
        'output_csv': OUTPUT_CSV,
        'no_victims_csv': NO_VICTIMS_CSV,
        'filtering_log': FILTERING_LOG,
        'stats_json': STATS_JSON,
        'default_output_csv': DEFAULT_OUTPUT_CSV,
        'processed_data_dir': PROCESSED_DATA_DIR,
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

def guardar_csv_incremental_with_controlled_paths(paths, artigos_filtrados, artigos_sem_vitimas=None):
    """
    Save to controlled output paths with better organization
    """
    # Save filtered articles with victims
    if artigos_filtrados:
        guardar_csv_incremental(paths['output_csv'], artigos_filtrados)
        guardar_csv_incremental(paths['default_output_csv'], artigos_filtrados)
        log_progress(f"✅ Filtered articles with victims saved: {len(artigos_filtrados)} articles")
    
    # Save articles without victims separately if provided
    if artigos_sem_vitimas:
        guardar_csv_incremental(paths['no_victims_csv'], artigos_sem_vitimas)
        log_progress(f"✅ Articles without victims saved: {len(artigos_sem_vitimas)} articles")
    
    # Save filtering statistics
    if artigos_filtrados or artigos_sem_vitimas:
        stats = {
            "filtering_date": datetime.now().isoformat(),
            "articles_with_victims": len(artigos_filtrados) if artigos_filtrados else 0,
            "articles_without_victims": len(artigos_sem_vitimas) if artigos_sem_vitimas else 0,
            "total_filtered": (len(artigos_filtrados) if artigos_filtrados else 0) + (len(artigos_sem_vitimas) if artigos_sem_vitimas else 0),
            "output_files": {
                "victims_articles": paths['output_csv'],
                "victims_articles_legacy": paths['default_output_csv'],
                "no_victims_articles": paths['no_victims_csv'] if artigos_sem_vitimas else None
            }
        }
        
        try:
            import json
            with open(paths['stats_json'], 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            log_progress(f"✅ Filtering statistics saved: {paths['stats_json']}")
        except Exception as e:
            log_progress(f"⚠️ Could not save statistics: {e}", "warning")

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

def enhanced_keyword_filter(text, keywords_dict):
    """Enhanced keyword filtering with pattern matching and context"""
    if not isinstance(text, str):
        return False, []
    
    text_lower = text.lower()
    matched_keywords = []
    
    # Check for positive keywords with context
    positive_patterns = keywords_dict.get('positive', [])
    negative_patterns = keywords_dict.get('negative', [])
    
    # Find positive matches
    for pattern in positive_patterns:
        if isinstance(pattern, dict):
            keyword = pattern['keyword']
            context = pattern.get('context', [])
            weight = pattern.get('weight', 1)
            
            if keyword in text_lower:
                # Check if negative context is present
                has_negative_context = any(neg in text_lower for neg in context)
                if not has_negative_context:
                    matched_keywords.append({'keyword': keyword, 'weight': weight})
        else:
            if pattern in text_lower:
                matched_keywords.append({'keyword': pattern, 'weight': 1})
    
    # Check for negative patterns that invalidate the article
    for neg_pattern in negative_patterns:
        if neg_pattern in text_lower:
            return False, []
    
    return len(matched_keywords) > 0, matched_keywords

def validate_victim_counts(row):
    """Validate victim counts to filter out false positives"""
    victim_fields = ["fatalities", "injured", "evacuated", "displaced", "missing"]
    
    # Check for reasonable victim counts (not impossibly high)
    max_reasonable_victims = 10000  # Adjust based on your data
    
    total_victims = 0
    for field in victim_fields:
        value = row.get(field, 0)
        if pd.notna(value) and value > 0:
            if value > max_reasonable_victims:
                return False  # Unreasonably high numbers
            total_victims += value
    
    # Check if total victims is reasonable
    if total_victims > max_reasonable_victims:
        return False
    
    return total_victims > 0

def content_quality_filter(row):
    """Filter based on content quality indicators"""
    url = str(row.get('page', '')).lower()
    title = str(row.get('title', '')).lower()
    
    # Quality indicators
    quality_checks = {
        'has_meaningful_url': len(url) > 20 and not url.endswith('/'),
        'has_title': len(title) > 10,
        'not_social_media': not any(social in url for social in ['facebook.com', 'twitter.com', 'instagram.com']),
        'not_forum': not any(forum in url for forum in ['forum', 'chat', 'comments']),
        'reputable_source': any(source in url for source in [
            'rtp.pt', 'publico.pt', 'dn.pt', 'tvi24.pt', 'sic.pt', 'cmjornal.pt',
            'jn.pt', 'record.pt', 'observador.pt', 'expresso.pt'
        ])
    }
    
    # Require at least 3 quality indicators
    quality_score = sum(quality_checks.values())
    return quality_score >= 3

def enhanced_geographic_filter(row, distritos_validos, paroquias_validas):
    """Enhanced geographic filtering with fuzzy matching"""
    distrito = str(row.get('district', '')).strip()
    parish = str(row.get('parish', '')).strip()
    url = str(row.get('page', '')).lower()
    
    # Normalize district and parish names
    distrito_normalized = distrito.title() if distrito else ""
    parish_normalized = parish.title() if parish else ""
    
    # Check exact matches first
    valid_district = distrito_normalized in distritos_validos
    valid_parish = not parish_normalized or parish_normalized in paroquias_validas
    
    # If exact match fails, try fuzzy matching for common variations
    if not valid_district and distrito:
        # Handle common district name variations
        distrito_variations = {
            'porto': 'Porto',
            'lisboa': 'Lisboa',
            'coimbra': 'Coimbra',
            'braga': 'Braga',
            'aveiro': 'Aveiro'
        }
        distrito_key = distrito.lower()
        if distrito_key in distrito_variations:
            valid_district = distrito_variations[distrito_key] in distritos_validos
    
    # Additional URL-based geographic validation
    portugal_indicators = [
        'portugal', '.pt', 'portugues', 'portuguesa', 'lusa', 'lusitania'
    ]
    has_portugal_indicator = any(indicator in url for indicator in portugal_indicators)
    
    # International exclusion (more comprehensive)
    international_exclusions = [
        'brasil', 'brazil', 'espanha', 'spain', 'franca', 'france', 'italia', 'italy',
        'alemanha', 'germany', 'inglaterra', 'england', 'holanda', 'netherlands',
        'belgica', 'belgium', 'suica', 'switzerland', 'austria', 'grecia', 'greece',
        'internacional', 'mundial', 'global', 'europa', 'america', 'africa', 'asia'
    ]
    is_international = any(exclusion in url for exclusion in international_exclusions)
    
    return valid_district and valid_parish and (has_portugal_indicator or not is_international)

def temporal_relevance_filter(row, target_date=None, max_age_days=365):
    """Filter based on temporal relevance"""
    if target_date is None:
        target_date = datetime.now()
    elif isinstance(target_date, str):
        target_date = datetime.strptime(target_date, "%Y-%m-%d")
    
    # Check article date
    article_date = None
    if 'date' in row and pd.notna(row['date']):
        try:
            if isinstance(row['date'], str):
                # Try multiple date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        article_date = datetime.strptime(row['date'], fmt)
                        break
                    except ValueError:
                        continue
            else:
                article_date = pd.to_datetime(row['date']).to_pydatetime()
        except:
            pass
    
    if article_date:
        days_old = (target_date - article_date).days
        return 0 <= days_old <= max_age_days
    
    return True  # If no date available, don't filter out

def calculate_enhanced_relevance_score(row):
    """Calculate enhanced relevance score with weighted keywords"""
    url = str(row.get('page', '')).lower()
    title = str(row.get('title', '')).lower()
    
    # Weighted keyword categories
    keyword_weights = {
        'victims': {'weight': 3, 'keywords': ['vitima', 'morto', 'morreu', 'faleceu', 'obito']},
        'injuries': {'weight': 2, 'keywords': ['ferido', 'ferimento', 'lesao', 'hospital']},
        'displacement': {'weight': 2, 'keywords': ['desalojado', 'evacuado', 'realojado', 'abrigo']},
        'missing': {'weight': 3, 'keywords': ['desaparecido', 'perdido', 'procura']},
        'weather_events': {'weight': 1, 'keywords': [
            'tempestade', 'temporal', 'inundacao', 'cheia', 'chuva', 'vento',
            'tornado', 'ciclone', 'granizo', 'neve', 'gelo', 'seca'
        ]},
        'infrastructure': {'weight': 1, 'keywords': [
            'estrada', 'ponte', 'casa', 'edificio', 'destruicao', 'dano'
        ]}
    }
    
    total_score = 0
    matched_categories = []
    
    combined_text = f"{url} {title}"
    
    for category, data in keyword_weights.items():
        weight = data['weight']
        keywords = data['keywords']
        
        category_matches = sum(1 for keyword in keywords if keyword in combined_text)
        if category_matches > 0:
            total_score += category_matches * weight
            matched_categories.append(category)
    
    return total_score, matched_categories

def apply_enhanced_comprehensive_filters(df, project_root, target_date=None):
    """Apply enhanced comprehensive filtering logic"""
    log_progress("🔧 Loading configuration data for enhanced filtering...")
    distritos_validos, paroquias_validas, eventos_climaticos = load_config_data(project_root)
    
    log_progress(f"📊 Starting enhanced comprehensive filtering with {len(df)} articles")
    
    # Make sure numeric columns are properly converted
    numeric_columns = ['fatalities', 'injured', 'evacuated', 'displaced', 'missing', 'year']
    df = safe_numeric_conversion(df, numeric_columns)
    
    # Enhanced geographic filter
    log_progress("🇵🇹 Applying enhanced geographic filter...")
    initial_count = len(df)
    df = df[df.apply(lambda row: enhanced_geographic_filter(row, distritos_validos, paroquias_validas), axis=1)]
    log_progress(f"   After enhanced geographic filter: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Content quality filter
    log_progress("✨ Applying content quality filter...")
    initial_count = len(df)
    df = df[df.apply(content_quality_filter, axis=1)]
    log_progress(f"   After quality filter: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Enhanced keyword filtering
    log_progress("🔍 Applying enhanced keyword filtering...")
    enhanced_keywords = {
        'positive': [
            {'keyword': 'vitima', 'context': ['simulacro', 'exercicio'], 'weight': 3},
            {'keyword': 'morto', 'context': ['filme', 'livro'], 'weight': 3},
            {'keyword': 'ferido', 'context': ['futebol', 'desporto'], 'weight': 2},
            'inundacao', 'temporal', 'tempestade', 'evacuado', 'desalojado'
        ],
        'negative': [
            'previsao', 'previsto', 'aviso', 'alerta', 'meteorologia',
            'historia', 'historico', 'retrospectiva', 'filme', 'livro',
            'desporto', 'futebol', 'simulacro', 'exercicio'
        ]
    }
    
    initial_count = len(df)
    if 'page' in df.columns:
        keyword_results = df['page'].apply(lambda url: enhanced_keyword_filter(url, enhanced_keywords))
        df = df[keyword_results.apply(lambda x: x[0])]  # Keep only articles that pass keyword filter
    log_progress(f"   After enhanced keyword filter: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Temporal relevance filter
    log_progress("📅 Applying temporal relevance filter...")
    initial_count = len(df)
    df = df[df.apply(lambda row: temporal_relevance_filter(row, target_date), axis=1)]
    log_progress(f"   After temporal filter: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Enhanced victim validation
    log_progress("👥 Applying enhanced victim validation...")
    initial_count = len(df)
    df = df[df.apply(validate_victim_counts, axis=1)]
    log_progress(f"   After victim validation: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Calculate enhanced relevance scores
    log_progress("⭐ Calculating enhanced relevance scores...")
    relevance_results = df.apply(calculate_enhanced_relevance_score, axis=1)
    df['relevance_score'] = relevance_results.apply(lambda x: x[0])
    df['matched_categories'] = relevance_results.apply(lambda x: x[1])
    
    # Filter by minimum relevance score
    min_relevance_score = 2
    initial_count = len(df)
    df = df[df['relevance_score'] >= min_relevance_score]
    log_progress(f"   After relevance score filter (min {min_relevance_score}): {len(df)} articles (removed {initial_count - len(df)})")
    
    # Remove duplicates with enhanced logic
    log_progress("🔄 Removing duplicates with enhanced logic...")
    if 'page' in df.columns:
        initial_count = len(df)
        # First remove exact URL duplicates
        df = df.drop_duplicates(subset=['page'])
        log_progress(f"   After URL deduplication: {len(df)} articles (removed {initial_count - len(df)})")
        
        # Then remove similar content duplicates
        initial_count = len(df)
        similarity_columns = ['district', 'parish', 'date', 'fatalities', 'injured']
        existing_columns = [col for col in similarity_columns if col in df.columns]
        if existing_columns:
            df = df.drop_duplicates(subset=existing_columns)
            log_progress(f"   After content similarity deduplication: {len(df)} articles (removed {initial_count - len(df)})")
    
    # Final cleanup
    df = df.dropna(how='all')
    
    # Add enhanced event identification
    log_progress("🌦️ Enhanced climate event identification...")
    if 'page' in df.columns:
        df["evento_nome"] = df["page"].apply(lambda url: identificar_evento(url, eventos_climaticos))
        df["evento_nome"] = df["evento_nome"].fillna("desconhecido")
    
    return df

def airflow_main(target_date=None, dias=1, input_file=None, output_dir=None, date_str=None):
    """
    Main function optimized for Airflow execution with enhanced filtering
    """
    log_progress("🚀 Starting enhanced filtrar_artigos_vitimas_airflow")
    
    # Setup paths and configuration with controlled paths
    try:
        paths = setup_paths_and_dates(target_date, dias, input_file, output_dir, date_str)
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
            "Processed Data Directory": paths['processed_data_dir'],
            "Days Filter": dias
        }
        log_statistics(initial_stats, "Enhanced Filtering Started")
        
        # Apply enhanced comprehensive filtering
        log_progress("🔍 Applying enhanced comprehensive filtering logic...")
        filtered_df = apply_enhanced_comprehensive_filters(df, paths['project_root'], target_date)
        
        if filtered_df.empty:
            log_progress("⚠️ No articles remaining after enhanced filtering.", "warning")
            return 0
        
        # Separate articles with and without victims for better organization
        victim_columns = ['fatalities', 'injured', 'evacuated', 'displaced', 'missing']
        has_victims_mask = filtered_df[victim_columns].sum(axis=1) > 0
        
        artigos_com_vitimas = filtered_df[has_victims_mask].to_dict('records')
        artigos_sem_vitimas = filtered_df[~has_victims_mask].to_dict('records')
        
        # Enhanced statistics
        comprehensive_stats = {
            "Articles After Enhanced Filters": len(filtered_df),
            "Articles with Victims": len(artigos_com_vitimas),
            "Articles without Victims": len(artigos_sem_vitimas),
            "Total Fatalities": filtered_df['fatalities'].sum(),
            "Total Injured": filtered_df['injured'].sum(),
            "Total Evacuated": filtered_df['evacuated'].sum(),
            "Total Displaced": filtered_df['displaced'].sum(),
            "Total Missing": filtered_df['missing'].sum(),
            "Average Relevance Score": f"{filtered_df['relevance_score'].mean():.2f}" if 'relevance_score' in filtered_df.columns else "N/A",
            "Top Event Types": filtered_df['evento_nome'].value_counts().head(3).to_dict() if 'evento_nome' in filtered_df.columns else {}
        }
        log_statistics(comprehensive_stats, "Enhanced Filtering Results")
        
        # Save filtered articles with controlled paths
        log_progress("💾 Saving enhanced filtered articles...")
        guardar_csv_incremental_with_controlled_paths(paths, artigos_com_vitimas, artigos_sem_vitimas)
        
        final_stats = {
            "Articles with Victims": len(artigos_com_vitimas),
            "Articles without Victims": len(artigos_sem_vitimas),
            "Total Articles Processed": len(filtered_df),
            "Enhanced Filter Rate": f"{len(filtered_df)/len(df)*100:.1f}%" if len(df) > 0 else "0%",
            "Victim Rate": f"{len(artigos_com_vitimas)/len(filtered_df)*100:.1f}%" if len(filtered_df) > 0 else "0%",
            "Main Output File": paths['output_csv'],
            "Statistics File": paths['stats_json']
        }
        log_statistics(final_stats, "Enhanced Filtering Completed Successfully")
        
        return len(artigos_com_vitimas)
        
    except FileNotFoundError as e:
        log_progress(f"❌ File not found error: {str(e)}", "error")
        return 0
    except Exception as e:
        log_progress(f"❌ Error during enhanced filtering: {str(e)}", "error")
        import traceback
        log_progress(f"❌ Full traceback: {traceback.format_exc()}", "error")
        raise

def main():
    """Main function with controlled output paths support"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Filter articles with victims (Airflow version)")
    parser.add_argument("--dias", type=int, default=1, help="Number of days to process")
    parser.add_argument("--date", type=str, help="Specific target date (YYYY-MM-DD)")
    parser.add_argument("--input_file", type=str, help="Specific input file path")
    parser.add_argument("--output_dir", type=str, help="Output directory for filtered files")
    parser.add_argument("--date_str", type=str, help="Date string for file naming")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Configure logging level
    if args.debug:
        logger.setLevel(logging.DEBUG)
        log_progress("🔍 DEBUG logging enabled", "debug")
    
    log_progress("Starting filtrar_artigos_vitimas_airflow")
    log_progress(f"Parameters: dias={args.dias}, date={args.date}")
    log_progress(f"Paths: input_file={args.input_file}, output_dir={args.output_dir}, date_str={args.date_str}")
    
    try:
        result = airflow_main(
            target_date=args.date, 
            dias=args.dias,
            input_file=args.input_file,
            output_dir=args.output_dir,
            date_str=args.date_str
        )
        log_progress(f"✅ Filtering completed. Found {result} articles with victims")
        return 0
    except Exception as e:
        log_progress(f"❌ Filtering failed: {e}", "error")
        return 1

if __name__ == "__main__":
    sys.exit(main())
