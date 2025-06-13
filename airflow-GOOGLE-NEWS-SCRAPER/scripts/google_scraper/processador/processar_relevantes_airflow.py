# File: airflow-GOOGLE-NEWS-SCRAPER/scripts/google_scraper/processador/filtrar_artigos_vitimas_airflow.py
# Script para processar artigos relevantes do Google News e filtrar aqueles relacionados a vítimas de desastres naturais

from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os
import random
import logging
import argparse
import time
import re
import hashlib
import requests
import traceback  # Add this missing import
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import urlparse
import pickle
import numpy as np
from pathlib import Path

# Add missing imports at the top
import json

# Set unbuffered output at the very beginning
os.environ['PYTHONUNBUFFERED'] = '1'
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(line_buffering=True)

# Add the missing imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.helpers import carregar_paroquias_com_municipios, load_keywords, carregar_dicofreg, guardar_csv_incremental, detect_municipality 
from extracao.extractor import resolve_google_news_url, fetch_and_extract_article_text
from extracao.normalizador import detect_disaster_type, extract_victim_counts, normalize, is_potentially_disaster_related
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# At the top, modify the ML import to be optional and safer
try:
    from .ml_enhanced_filter import MLEnhancedFilter
    ML_AVAILABLE = True
except ImportError:
    try:
        from scripts.google_scraper.processador.ml_enhanced_filter import MLEnhancedFilter
        ML_AVAILABLE = True
    except ImportError:
        MLEnhancedFilter = None
        ML_AVAILABLE = False
        print("⚠️ ML Enhanced Filter not available - continuing without ML filtering")

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
    
    logger = logging.getLogger("processar_relevantes_airflow")
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
    Find the input CSV file, checking multiple possible locations
    to match the output structure from run_scraper_airflow.py
    """
    current_date = target_date.strftime("%Y%m%d")
    current_year = target_date.strftime("%Y")
    current_month = target_date.strftime("%m")
    current_day = target_date.strftime("%d")
    
    # Possible input file locations in order of preference
    possible_paths = [
        # Date-specific organized structure (matches run_scraper_airflow.py output)
        os.path.join(project_root, "data", "raw", current_year, current_month, current_day, f"intermediate_google_news_{current_date}.csv"),
        # Fallback to raw directory with date
        os.path.join(project_root, "data", "raw", f"intermediate_google_news_{current_date}.csv"),
        # Default fallback
        os.path.join(project_root, "data", "raw", "intermediate_google_news.csv")
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            log_progress(f"✅ Found input file: {path}")
            return path
    
    log_progress(f"❌ No input file found. Searched in:", "error")
    for path in possible_paths:
        log_progress(f"   - {path}", "error")
    
    return None

# Use date-specific filenames and setup paths
def setup_paths_and_dates(target_date=None, dias=1, input_file=None, output_dir=None, date_str=None):
    """Setup all paths and dates for processing with controlled output paths"""
    if target_date:
        if isinstance(target_date, str):
            dt = datetime.strptime(target_date, "%Y-%m-%d")
        else:
            dt = target_date
    else:
        dt = datetime.now() - timedelta(days=dias-1)  # Adjust for current day processing
    
    current_date = dt.strftime("%Y%m%d")
    current_year = dt.strftime("%Y")
    current_month = dt.strftime("%m")
    current_day = dt.strftime("%d")
    date_suffix = date_str if date_str else dt.strftime("%Y-%m-%d")  # Use provided date_str or default format
    
    # Get the directory where this script is located
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # Fix project root calculation for Docker environment
    # /opt/airflow/scripts/google_scraper/processador -> /opt/airflow
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))
    
    # Use controlled output directory if provided
    if output_dir:
        STRUCTURED_DIR = output_dir
        RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw", current_year, current_month, current_day)
        log_progress(f"📁 Using controlled output directory: {STRUCTURED_DIR}")
    else:
        # Fallback to default structure
        RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw", current_year, current_month, current_day)
        STRUCTURED_DIR = os.path.join(PROJECT_ROOT, "data", "structured", current_year, current_month, current_day)
    
    # Ensure directories exist
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(STRUCTURED_DIR, exist_ok=True)
    
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
    OUTPUT_CSV = os.path.join(RAW_DATA_DIR, f"artigos_google_municipios_pt_{date_suffix}.csv")
    DEFAULT_OUTPUT_CSV = os.path.join(STRUCTURED_DIR, f"artigos_google_municipios_pt_{date_suffix}.csv")
    
    # Additional output files for better organization
    IRRELEVANT_CSV = os.path.join(STRUCTURED_DIR, f"artigos_irrelevantes_{date_suffix}.csv")
    PROCESSING_LOG = os.path.join(STRUCTURED_DIR, f"processing_log_{current_date}.log")
    STATS_JSON = os.path.join(STRUCTURED_DIR, f"processing_stats_{current_date}.json")
    
    return {
        'input_csv': INPUT_CSV,
        'output_csv': OUTPUT_CSV,
        'default_output_csv': DEFAULT_OUTPUT_CSV,
        'irrelevant_csv': IRRELEVANT_CSV,
        'processing_log': PROCESSING_LOG,
        'stats_json': STATS_JSON,
        'raw_data_dir': RAW_DATA_DIR,
        'structured_dir': STRUCTURED_DIR,
        'project_root': PROJECT_ROOT,
        'current_date': current_date,
        'dt': dt
    }

# Load configuration data
def load_configuration():
    """Load all configuration data needed for processing"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Fix project root calculation for Docker environment
        # /opt/airflow/scripts/google_scraper/processador -> /opt/airflow
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
        
        # Try multiple paths for config files - more comprehensive search
        config_paths = [
            os.path.join(project_root, "config"),
            os.path.join(project_root, "simprede-airflow", "config"),
            os.path.join(script_dir, "..", "..", "..", "config"),
            os.path.join(script_dir, "..", "config"),
            os.path.join(script_dir, "config"),
            "/opt/airflow/config",
            "/opt/airflow/simprede-airflow/config",
            "config"
        ]
        
        log_progress("🔧 A carregar ficheiros de configuração...")
        
        LOCALIDADES = {}
        MUNICIPIOS = []
        DISTRITOS = []
        KEYWORDS = []
        FREGUESIAS_COM_CODIGOS = {}
        
        configs_loaded = {
            'locations': False,
            'keywords': False,
            'freguesias': False
        }
        
        for config_path in config_paths:
            log_progress(f"🔍 Checking config path: {config_path}")
            
            # Load locations
            locations_file = os.path.join(config_path, "municipios_por_distrito.json")
            if os.path.exists(locations_file) and not configs_loaded['locations']:
                log_progress(f"📁 Loading locations from: {locations_file}")
                LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios(locations_file)
                configs_loaded['locations'] = True
                log_progress(f"✅ Loaded {len(LOCALIDADES)} parishes, {len(MUNICIPIOS)} municipalities, {len(DISTRITOS)} districts")
            
            # Load keywords
            keywords_file = os.path.join(config_path, "keywords.json")
            if os.path.exists(keywords_file) and not configs_loaded['keywords']:
                log_progress(f"📁 Loading keywords from: {keywords_file}")
                KEYWORDS = load_keywords(keywords_file, idioma="portuguese")
                configs_loaded['keywords'] = True
                log_progress(f"✅ Loaded {len(KEYWORDS)} keywords")
            
            if all(v for k, v in configs_loaded.items() if k != 'freguesias'):
                break
        
        # Load DICOFREG (this might be in a different location)
        try:
            FREGUESIAS_COM_CODIGOS = carregar_dicofreg()
            configs_loaded['freguesias'] = True
            log_progress(f"✅ Loaded {len(FREGUESIAS_COM_CODIGOS)} DICOFREG codes")
        except Exception as e:
            log_progress(f"⚠️ Could not load DICOFREG codes: {e}", "warning")
        
        # Verify all required configs are loaded
        missing_configs = [k for k, v in configs_loaded.items() if not v and k != 'freguesias']
        if missing_configs:
            raise Exception(f"Missing required configurations: {missing_configs}")
        
        config_stats = {
            "Parishes": len(LOCALIDADES),
            "Municipalities": len(MUNICIPIOS),
            "Districts": len(DISTRITOS),
            "Keywords": len(KEYWORDS),
            "DICOFREG Codes": len(FREGUESIAS_COM_CODIGOS)
        }
        log_statistics(config_stats, "Configuration Loaded")
        
        return LOCALIDADES, MUNICIPIOS, DISTRITOS, KEYWORDS, FREGUESIAS_COM_CODIGOS
        
    except Exception as e:
        log_progress(f"❌ Error loading configuration: {e}", "error")
        raise

class DynamicRateLimiter:
    def __init__(self):
        self.last_access = defaultdict(float)
        self.domain_delays = defaultdict(lambda: 2.0)  # Default 2 second delay
    
    def wait_if_needed(self, url):
        domain = urlparse(url).netloc
        elapsed = time.time() - self.last_access[domain]
        delay = self.domain_delays[domain]
        
        if elapsed < delay:
            wait_time = delay - elapsed
            time.sleep(wait_time)
        
        self.last_access[domain] = time.time()
    
    def adjust_delay(self, url, success):
        domain = urlparse(url).netloc
        if success:
            # Gradually reduce delay for successful requests (but keep minimum)
            self.domain_delays[domain] = max(1.0, self.domain_delays[domain] * 0.9)
        else:
            # Increase delay for failed requests
            self.domain_delays[domain] = min(60.0, self.domain_delays[domain] * 2)

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

def is_international_news(title, url):
    """
    Verifica se a notícia é sobre um evento internacional
    """
    # Lista de países e regiões estrangeiras para detectar
    foreign_countries = [
        "frança", "franca", "espanha", "australia", "austrália",  "paquistão", "paquistao",
        "itália", "italia", "nepal", "índia", "india", "china", 
        "estados unidos", "eua", "brasil", "rússia", "russia",
        "japão", "japao", "alemanha", "reino unido", "bélgica", "belgica"
    ]
    
    title_lower = title.lower()
    url_lower = url.lower()
    
    # Verifica se o título ou URL menciona um país estrangeiro
    for country in foreign_countries:
        if country in title_lower or country in url_lower:
            return True
    
    # Verifica por indicadores de notícias internacionais
    international_indicators = ["internacional", "mundo", "global"]
    for indicator in international_indicators:
        if indicator in url_lower:
            return True
    
    return False

# Complete the extract_victims_from_title function
def extract_victims_from_title(title):
    """Extract victim counts from article title using Portuguese patterns"""
    vitimas = {
        "fatalities": 0,
        "injured": 0,
        "evacuated": 0,
        "displaced": 0,
        "missing": 0
    }
    
    if not title or pd.isna(title):
        return vitimas
    
    title_lower = title.lower()
    
    # Portuguese victim patterns
    fatalities_patterns = [
        r'(\d+)\s*mort[oa]s?',
        r'(\d+)\s*vítimas?\s*mortais?',
        r'(\d+)\s*óbitos?',
        r'(?:mata|matou|morr[eu])\s*(\d+)',
        r'(\d+)\s*pessoa[s]?\s*(?:morr[eu]|falec[eu])',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*mort[oa]s?',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*vítimas?\s*mortais?'
    ]
    
    injured_patterns = [
        r'(\d+)\s*ferid[oa]s?',
        r'(\d+)\s*pessoa[s]?\s*ferid[ao]s?',
        r'(?:fere|deixa)\s*(\d+)',
        r'(\d+)\s*ferimento[s]?',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*ferid[oa]s?'
    ]
    
    evacuated_patterns = [
        r'(\d+)\s*evacuad[oa]s?',
        r'(\d+)\s*pessoa[s]?\s*evacuad[ao]s?',
        r'evacuação\s*de\s*(\d+)',
        r'(?:retirou|retirad[oa]s?)\s*(\d+)',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*evacuad[oa]s?'
    ]
    
    displaced_patterns = [
        r'(\d+)\s*desalojad[oa]s?',
        r'(\d+)\s*pessoa[s]?\s*desalojad[ao]s?',
        r'(\d+)\s*(?:sem\s*casa|sem\s*abrigo)',
        r'(?:realojad[oa]s?|realojamento)\s*(\d+)',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*desalojad[oa]s?'
    ]
    
    missing_patterns = [
        r'(\d+)\s*desaparecid[oa]s?',
        r'(\d+)\s*pessoa[s]?\s*desaparecid[ao]s?',
        r'(?:procuram?|procura[s]?)\s*(\d+)',
        r'(\d+)\s*(?:em\s*falta|dado[s]?\s*como\s*desaparecid[oa]s?)',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*desaparecid[oa]s?'
    ]
    
    # Number word mappings
    num_words = {
        "um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "tres": 3,
        "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8,
        "nove": 9, "dez": 10, "onze": 11, "doze": 12, "vinte": 20
    }
    
    def extract_number(match_text):
        """Extract number from match, handling both digits and words"""
        if match_text.isdigit():
            return int(match_text)
        elif match_text.lower() in num_words:
            return num_words[match_text.lower()]
        return 0
    
    # Extract fatalities
    for pattern in fatalities_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):  # If there's a capture group
                    num = extract_number(match.group(1))
                else:
                    # Handle patterns without capture groups
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["fatalities"] = max(vitimas["fatalities"], num)
            except (ValueError, IndexError):
                pass
    
    # Extract injured
    for pattern in injured_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):
                    num = extract_number(match.group(1))
                else:
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["injured"] = max(vitimas["injured"], num)
            except (ValueError, IndexError):
                pass
    
    # Extract evacuated
    for pattern in evacuated_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):
                    num = extract_number(match.group(1))
                else:
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["evacuated"] = max(vitimas["evacuated"], num)
            except (ValueError, IndexError):
                pass
    
    # Extract displaced
    for pattern in displaced_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):
                    num = extract_number(match.group(1))
                else:
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["displaced"] = max(vitimas["displaced"], num)
            except (ValueError, IndexError):
                pass
    
    # Extract missing
    for pattern in missing_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):
                    num = extract_number(match.group(1))
                else:
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["missing"] = max(vitimas["missing"], num)
            except (ValueError, IndexError):
                pass
    
    return vitimas

def processar_artigo(row, rate_limiter, LOCALIDADES, KEYWORDS, FREGUESIAS_COM_CODIGOS, progress_callback=None):
    """Process a single article with enhanced error handling"""
    try:
        # Extract data from the row with safer field access
        original_url = str(row.get("link", "")).strip()
        titulo = str(row.get("title", "")).strip()
        localidade = str(row.get("localidade", "")).strip()
        keyword = str(row.get("keyword", "desconhecido")).strip()
        publicado = str(row.get("published", "")).strip()
        article_id = str(row.get("ID", "")).strip()
        
        # Generate a fallback ID if missing
        if not article_id:
            import hashlib
            article_id = hashlib.md5(f"{original_url}{titulo}".encode()).hexdigest()[:16]
        
        # Validate required fields
        if not all([original_url, titulo]):
            log_progress(f"⚠️ Artigo com dados incompletos ignorado: ID={article_id}, título={titulo[:30]}...", "warning")
            return None

        # OPTIMIZATION: Pre-filter based on title analysis
        non_disaster_indicators = [
            'futebol', 'desporto', 'filme', 'música', 'exposição', 'festival',
            'política', 'economia', 'eleições', 'parlamento', 'governo',
            'arte', 'cultura', 'literatura', 'teatro', 'cinema', 'turismo',
            'saúde', 'médico', 'hospital', 'educação', 'escola'
        ]
        
        titulo_lower = titulo.lower()
        if any(indicator in titulo_lower for indicator in non_disaster_indicators):
            # Quick check if it might still be disaster-related
            disaster_keywords = ['incêndio', 'inundação', 'temporal', 'vento', 'chuva', 'neve', 'morto', 'ferido', 'evacuado', 'granizo', 'trovoada']
            if not any(keyword in titulo_lower for keyword in disaster_keywords):
                log_progress(f"⚠️ Artigo filtrado por título não relacionado: {titulo[:50]}...", "warning")
                return None

        # Check for victims in the title first
        vitimas_do_titulo = extract_victims_from_title(titulo)
        has_victims_in_title = any(vitimas_do_titulo.values())
        
        # Try to detect disaster type from title
        potential_tipo_from_title, subtipo_from_title = detect_disaster_type(titulo)
        
        if has_victims_in_title:
            log_progress(f"✅ Vítimas encontradas no título: {titulo}")
        
        if potential_tipo_from_title != "unknown":
            log_progress(f"🔍 Tipo de desastre identificado do título: {potential_tipo_from_title}")
        
        # URL resolution with better error handling
        resolved_url = None
        try:
            rate_limiter.wait_if_needed(original_url)
            resolved_url = resolve_google_news_url(original_url)
            if not resolved_url or not resolved_url.startswith("http"):
                log_progress(f"⚠️ Link não resolvido: {original_url[:100]}...", "warning")
                # If we have victims and disaster type from title, create partial record
                if has_victims_in_title and potential_tipo_from_title != "unknown":
                    log_progress(f"💡 Criando registro parcial baseado apenas no título")
                    return create_partial_article_record(
                        article_id, titulo, original_url, localidade, keyword, publicado,
                        potential_tipo_from_title, subtipo_from_title, vitimas_do_titulo,
                        LOCALIDADES, FREGUESIAS_COM_CODIGOS
                    )
                return None
        except Exception as e:
            log_progress(f"❌ Erro de conexão ao resolver URL: {str(e)[:100]}...", "error")
            # If we have victims and disaster type from title, create partial record
            if has_victims_in_title and potential_tipo_from_title != "unknown":
                log_progress(f"💡 Criando registro parcial baseado apenas no título")
                return create_partial_article_record(
                    article_id, titulo, original_url, localidade, keyword, publicado,
                    potential_tipo_from_title, subtipo_from_title, vitimas_do_titulo,
                    LOCALIDADES, FREGUESIAS_COM_CODIGOS
                )
            return None

        # Fetch article text with better error handling
        texto = ""
        if not has_victims_in_title or potential_tipo_from_title == "unknown":
            try:
                rate_limiter.wait_if_needed(resolved_url)
                texto = fetch_and_extract_article_text(resolved_url)
                if not texto or not is_potentially_disaster_related(texto, KEYWORDS):
                    log_progress(f"⚠️ Artigo ignorado após extração: {titulo[:50]}...", "warning")
                    return None
            except Exception as e:
                log_progress(f"❌ Erro de conexão ao extrair texto: {str(e)[:100]}...", "error")
                return None
        
        # Determine disaster type
        if potential_tipo_from_title and potential_tipo_from_title != "unknown":
            tipo, subtipo = potential_tipo_from_title, subtipo_from_title
        else:
            tipo, subtipo = detect_disaster_type(texto or titulo)
        
        # Extract victims
        if has_victims_in_title:
            vitimas = vitimas_do_titulo
        else:
            vitimas = extract_victim_counts(texto)
            if not any(vitimas.values()):
                vitimas = extract_victims_from_title(titulo)

        # Geographic detection
        loc = detect_municipality(texto or titulo, LOCALIDADES) or localidade
        district = LOCALIDADES.get(loc.lower(), {}).get("district", "")
        concelho = LOCALIDADES.get(loc.lower(), {}).get("municipality", "")
        parish_normalized = normalize(loc.lower())
        dicofreg = FREGUESIAS_COM_CODIGOS.get(parish_normalized, "")

        # Date formatting
        data_evt_formatada, ano, mes, dia, hora_evt = formatar_data_para_ddmmyyyy(publicado)
        fonte = extrair_nome_fonte(resolved_url)

        # Create clean result record
        result = {
            "ID": article_id,
            "title": clean_text_for_csv(titulo),
            "type": tipo,
            "subtype": subtipo,
            "date": data_evt_formatada,
            "year": ano,
            "month": mes,
            "day": dia,
            "hour": hora_evt,
            "georef": clean_text_for_csv(loc),
            "district": clean_text_for_csv(district),
            "municipali": clean_text_for_csv(concelho),
            "parish": clean_text_for_csv(loc),
            "DICOFREG": dicofreg,
            "source": clean_text_for_csv(fonte),
            "sourcedate": datetime.today().date().isoformat(),
            "sourcetype": "web",
            "page": clean_text_for_csv(resolved_url),
            "fatalities": vitimas["fatalities"],
            "injured": vitimas["injured"],
            "evacuated": vitimas["evacuated"],
            "displaced": vitimas["displaced"],
            "missing": vitimas["missing"]
        }

        log_progress(f"🔄 Processado: {titulo[:50]}...")
        return result
        
    except Exception as e:
        log_progress(f"❌ Erro inesperado ao processar artigo: {e}", "error")
        return None

def clean_text_for_csv(text):
    """Clean text to avoid CSV parsing issues"""
    if not text or pd.isna(text):
        return ""
    
    text = str(text).strip()
    # Remove problematic characters that can break CSV parsing
    text = text.replace('"', "'").replace('\n', ' ').replace('\r', ' ')
    # Remove multiple spaces
    text = ' '.join(text.split())
    return text

def create_partial_article_record(article_id, titulo, original_url, localidade, keyword, publicado,
                                 tipo, subtipo, vitimas, LOCALIDADES, FREGUESIAS_COM_CODIGOS):
    """Create a partial article record based on title information only"""
    
    loc = localidade
    district = LOCALIDADES.get(loc.lower(), {}).get("district", "")
    concelho = LOCALIDADES.get(loc.lower(), {}).get("municipality", "")
    parish_normalized = normalize(loc.lower())
    dicofreg = FREGUESIAS_COM_CODIGOS.get(parish_normalized, "")

    data_evt_formatada, ano, mes, dia, hora_evt = formatar_data_para_ddmmyyyy(publicado)
    fonte = extrair_nome_fonte(original_url)

    return {
        "ID": article_id,
        "title": clean_text_for_csv(titulo),
        "type": tipo,
        "subtype": subtipo,
        "date": data_evt_formatada,
        "year": ano,
        "month": mes,
        "day": dia,
        "hour": hora_evt,
        "georef": clean_text_for_csv(loc),
        "district": clean_text_for_csv(district),
        "municipali": clean_text_for_csv(concelho),
        "parish": clean_text_for_csv(loc),
        "DICOFREG": dicofreg,
        "source": clean_text_for_csv(fonte),
        "sourcedate": datetime.today().date().isoformat(),
        "sourcetype": "web",
        "page": clean_text_for_csv(original_url),
        "fatalities": vitimas["fatalities"],
        "injured": vitimas["injured"],
        "evacuated": vitimas["evacuated"],
        "displaced": vitimas["displaced"],
        "missing": vitimas["missing"]
    }
    
# Carregar links já importados
def carregar_links_existentes(output_csv):
    if not os.path.exists(output_csv):
        return set()
    try:
        df_existente = pd.read_csv(output_csv)
        return set(df_existente["page"].dropna().unique())
    except Exception:
        return set()

def is_duplicate_content(title, url, existing_titles, existing_urls):
    """Check if content is likely duplicate based on title similarity or URL patterns"""
    title_hash = hashlib.md5(title.lower().encode()).hexdigest()
    
    # Check for exact title match
    if title_hash in existing_titles:
        return True
    
    # Check for URL pattern match (removing query parameters)
    base_url = url.split('?')[0]
    if base_url in existing_urls:
        return True
    
    # Update caches
    existing_titles.add(title_hash)
    existing_urls.add(base_url)
    return False

def check_internet_connection():
    """Check if internet connection is available"""
    try:
        # Try to connect to a reliable server
        requests.get("https://www.google.com", timeout=5)
        return True
    except requests.RequestException:
        return False

def create_optimized_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # Increased from 3
        backoff_factor=2,  # Increased from 1
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, 
                          pool_connections=10, 
                          pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    })
    # Add timeouts to prevent hanging
    session.timeout = (10, 30)  # (connect, read) timeouts
    return session

def guardar_csv_incremental_with_date(output_csv, default_output_csv, artigos):
    """
    Save to date-specific file in the raw data directory with year/month/day structure
    and also maintain backward compatibility with structured directory
    """
    # Save to raw data directory (this will be organized by year/month/day)
    guardar_csv_incremental(output_csv, artigos)
    
    # Also save to standard file for backward compatibility
    guardar_csv_incremental(default_output_csv, artigos)
    
    log_progress(f"✅ Files saved with year/month/day organization")

def guardar_csv_incremental_with_controlled_paths(paths, artigos, irrelevant_articles=None):
    """
    Save to controlled output paths with better organization
    """
    # Save relevant articles to both raw and structured directories
    if artigos:
        guardar_csv_incremental(paths['output_csv'], artigos)
        guardar_csv_incremental(paths['default_output_csv'], artigos)
        log_progress(f"✅ Relevant articles saved: {len(artigos)} articles")
    
    # Save irrelevant articles separately if provided
    if irrelevant_articles:
        guardar_csv_incremental(paths['irrelevant_csv'], irrelevant_articles)
        log_progress(f"✅ Irrelevant articles saved: {len(irrelevant_articles)} articles")
    
    # Save processing statistics
    if artigos or irrelevant_articles:
        stats = {
            "processing_date": datetime.now().isoformat(),
            "relevant_articles": len(artigos) if artigos else 0,
            "irrelevant_articles": len(irrelevant_articles) if irrelevant_articles else 0,
            "total_processed": (len(artigos) if artigos else 0) + (len(irrelevant_articles) if irrelevant_articles else 0),
            "output_files": {
                "relevant_raw": paths['output_csv'],
                "relevant_structured": paths['default_output_csv'],
                "irrelevant": paths['irrelevant_csv'] if irrelevant_articles else None
            }
        }
        
        try:
            with open(paths['stats_json'], 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            log_progress(f"✅ Processing statistics saved: {paths['stats_json']}")
        except Exception as e:
            log_progress(f"⚠️ Could not save statistics: {e}", "warning")

# Add a progress callback function
def progress_update(message):
    """Progress callback for article processing"""
    log_progress(f"🔄 {message}")

def apply_ml_pre_filtering(df, project_root):
    """Apply ML filtering before detailed processing to save time"""
    if not ML_AVAILABLE or MLEnhancedFilter is None:
        log_progress("⚠️ ML Enhanced Filter not available, continuing with original order")
        return df
    
    try:
        # Try to import and use ML filtering
        ml_filter = MLEnhancedFilter()
        if ml_filter.load_models():
            log_progress("🤖 Applying ML pre-filtering to prioritize relevant articles...")
            
            ml_predictions, ml_scores = ml_filter.predict_relevance(
                df, 
                threshold=0.3  # Lower threshold for pre-filtering
            )
            
            # Sort by ML score (highest first)
            df['ml_temp_score'] = ml_scores
            df_sorted = df.sort_values('ml_temp_score', ascending=False)
            df_sorted = df_sorted.drop('ml_temp_score', axis=1)
            
            log_progress(f"📊 Articles sorted by ML relevance score")
            return df_sorted
        else:
            log_progress("⚠️ ML models not available, skipping ML pre-filtering")
            return df
            
    except Exception as e:
        log_progress(f"⚠️ ML pre-filtering failed: {e}, continuing with original order")
        return df

def airflow_main(target_date=None, dias=1, input_file=None, output_dir=None, date_str=None):
    """Main function for Airflow execution with ML filtering integration"""
    
    print("🔧 Setting up paths and configuration...")
    
    try:
        # Setup paths
        paths = setup_paths_and_dates(
            target_date=target_date,
            dias=dias,
            input_file=input_file,
            output_dir=output_dir,
            date_str=date_str
        )
        print(f"✅ Paths configured successfully")
        print(f"📂 Project root: {paths.get('project_root', 'Not set')}")
        print(f"📄 Input CSV: {paths.get('input_csv', 'Not set')}")
        
    except Exception as e:
        print(f"❌ Failed to setup paths: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    try:
        # Load configuration - FIX: Unpack all 5 values
        print("🔧 Loading configuration data...")
        LOCALIDADES, MUNICIPIOS, DISTRITOS, KEYWORDS, FREGUESIAS_COM_CODIGOS = load_configuration()
        print(f"✅ Configuration loaded: {len(LOCALIDADES)} localities, {len(KEYWORDS)} keywords")
        
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    try:
        # Read input file
        print(f"📖 Reading input file: {paths['input_csv']}")
        df = pd.read_csv(paths['input_csv'])
        print(f"✅ Loaded {len(df)} articles from input file")
        print(f"📊 Columns: {list(df.columns)}")
        
        if len(df) == 0:
            print("⚠️ No articles to process")
            return True
            
    except Exception as e:
        print(f"❌ Failed to read input file: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Apply ML pre-filtering to prioritize articles
    try:
        print("🤖 Applying ML pre-filtering...")
        df = apply_ml_pre_filtering(df, paths['project_root'])
        print(f"✅ ML pre-filtering completed")
    except Exception as e:
        print(f"⚠️ ML pre-filtering failed: {e}, continuing without ML filtering")
    
    # Continue with processing...
    print("🔄 Starting article processing...")
    
    # Check internet connection
    if not check_internet_connection():
        print("❌ No internet connection available")
        return False
    
    print(f"📊 Processing {len(df)} articles...")
    
    # Initialize rate limiter and session
    rate_limiter = DynamicRateLimiter()
    session = create_optimized_session()
    
    # Track processed articles
    artigos_processados = []
    artigos_irrelevantes = []
    existing_links = carregar_links_existentes(paths['output_csv'])
    existing_titles = set()
    existing_urls = set()
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    
    try:
        # Process articles with progress tracking
        for index, row in df.iterrows():
            try:
                # Progress indicator
                if processed_count % 10 == 0:
                    progress_pct = (processed_count / len(df)) * 100
                    print(f"🔄 Progress: {processed_count}/{len(df)} ({progress_pct:.1f}%)")
                
                # Skip duplicates
                if is_duplicate_content(row.get('title', ''), row.get('link', ''), existing_titles, existing_urls):
                    skipped_count += 1
                    continue
                
                # Skip if already processed
                if row.get('link', '') in existing_links:
                    skipped_count += 1
                    continue
                
                # Process the article
                resultado = processar_artigo(
                    row, 
                    rate_limiter, 
                    LOCALIDADES, 
                    KEYWORDS, 
                    FREGUESIAS_COM_CODIGOS, 
                    progress_callback=progress_update
                )
                
                if resultado:
                    # Check if article is relevant (has disaster type or victims)
                    if (resultado.get('type') != 'unknown' or 
                        any(resultado.get(col, 0) > 0 for col in ['fatalities', 'injured', 'evacuated', 'displaced', 'missing'])):
                        artigos_processados.append(resultado)
                        print(f"✅ Relevant article processed: {resultado.get('ID', 'Unknown ID')}")
                    else:
                        # Store irrelevant articles separately
                        artigos_irrelevantes.append(resultado)
                        print(f"ℹ️ Irrelevant article stored: {resultado.get('ID', 'Unknown ID')}")
                else:
                    error_count += 1
                
                processed_count += 1
                
                # Add small delay to avoid overwhelming servers
                time.sleep(0.1)
                
            except Exception as e:
                error_count += 1
                print(f"❌ Error processing article {index}: {e}")
                continue
        
        # Final statistics
        final_stats = {
            "Total Articles": len(df),
            "Processed Successfully": processed_count,
            "Relevant Articles": len(artigos_processados),
            "Irrelevant Articles": len(artigos_irrelevantes),
            "Skipped (Duplicates)": skipped_count,
            "Errors": error_count,
            "Success Rate": f"{(processed_count/len(df)*100):.1f}%" if len(df) > 0 else "0%"
        }
        
        print("📊 Final Processing Statistics:")
        for key, value in final_stats.items():
            print(f"   {key}: {value}")
        
        # Save results using controlled paths
        print("💾 Saving processed articles...")
        guardar_csv_incremental_with_controlled_paths(
            paths, 
            artigos_processados, 
            artigos_irrelevantes
        )
        
        print(f"✅ Processing completed successfully!")
        print(f"📊 Total relevant articles: {len(artigos_processados)}")
        print(f"📁 Output files saved to: {paths['structured_dir']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during article processing: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        if 'session' in locals():
            session.close()

# Update the apply_ml_pre_filtering function to handle import errors gracefully
def apply_ml_pre_filtering(df, project_root):
    """Apply ML filtering before detailed processing to save time"""
    if not ML_AVAILABLE or MLEnhancedFilter is None:
        log_progress("⚠️ ML Enhanced Filter not available, continuing with original order")
        return df
    
    try:
        # Try to import and use ML filtering
        ml_filter = MLEnhancedFilter()
        if ml_filter.load_models():
            log_progress("🤖 Applying ML pre-filtering to prioritize relevant articles...")
            
            ml_predictions, ml_scores = ml_filter.predict_relevance(
                df, 
                threshold=0.3  # Lower threshold for pre-filtering
            )
            
            # Sort by ML score (highest first)
            df['ml_temp_score'] = ml_scores
            df_sorted = df.sort_values('ml_temp_score', ascending=False)
            df_sorted = df_sorted.drop('ml_temp_score', axis=1)
            
            log_progress(f"📊 Articles sorted by ML relevance score")
            return df_sorted
        else:
            log_progress("⚠️ ML models not available, skipping ML pre-filtering")
            return df
            
    except Exception as e:
        log_progress(f"⚠️ ML pre-filtering failed: {e}, continuing with original order")
        return df

def extract_victims_from_title(title):
    """Extract victim counts from article title using Portuguese patterns"""
    vitimas = {
        "fatalities": 0,
        "injured": 0,
        "evacuated": 0,
        "displaced": 0,
        "missing": 0
    }
    
    if not title or pd.isna(title):
        return vitimas
    
    title_lower = title.lower()
    
    # Portuguese victim patterns
    fatalities_patterns = [
        r'(\d+)\s*mort[oa]s?',
        r'(\d+)\s*vítimas?\s*mortais?',
        r'(\d+)\s*óbitos?',
        r'(?:mata|matou|morr[eu])\s*(\d+)',
        r'(\d+)\s*pessoa[s]?\s*(?:morr[eu]|falec[eu])',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*mort[oa]s?',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*vítimas?\s*mortais?'
    ]
    
    injured_patterns = [
        r'(\d+)\s*ferid[oa]s?',
        r'(\d+)\s*pessoa[s]?\s*ferid[ao]s?',
        r'(?:fere|deixa)\s*(\d+)',
        r'(\d+)\s*ferimento[s]?',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*ferid[oa]s?'
    ]
    
    evacuated_patterns = [
        r'(\d+)\s*evacuad[oa]s?',
        r'(\d+)\s*pessoa[s]?\s*evacuad[ao]s?',
        r'evacuação\s*de\s*(\d+)',
        r'(?:retirou|retirad[oa]s?)\s*(\d+)',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*evacuad[oa]s?'
    ]
    
    displaced_patterns = [
        r'(\d+)\s*desalojad[oa]s?',
        r'(\d+)\s*pessoa[s]?\s*desalojad[ao]s?',
        r'(\d+)\s*(?:sem\s*casa|sem\s*abrigo)',
        r'(?:realojad[oa]s?|realojamento)\s*(\d+)',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*desalojad[oa]s?'
    ]
    
    missing_patterns = [
        r'(\d+)\s*desaparecid[oa]s?',
        r'(\d+)\s*pessoa[s]?\s*desaparecid[ao]s?',
        r'(?:procuram?|procura[s]?)\s*(\d+)',
        r'(\d+)\s*(?:em\s*falta|dado[s]?\s*como\s*desaparecid[oa]s?)',
        r'(?:um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s*desaparecid[oa]s?'
    ]
    
    # Number word mappings
    num_words = {
        "um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "tres": 3,
        "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8,
        "nove": 9, "dez": 10, "onze": 11, "doze": 12, "vinte": 20
    }
    
    def extract_number(match_text):
        """Extract number from match, handling both digits and words"""
        if match_text.isdigit():
            return int(match_text)
        elif match_text.lower() in num_words:
            return num_words[match_text.lower()]
        return 0
    
    # Extract fatalities
    for pattern in fatalities_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):  # If there's a capture group
                    num = extract_number(match.group(1))
                else:
                    # Handle patterns without capture groups
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["fatalities"] = max(vitimas["fatalities"], num)
            except (ValueError, IndexError):
                pass
    
    # Extract injured
    for pattern in injured_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):
                    num = extract_number(match.group(1))
                else:
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["injured"] = max(vitimas["injured"], num)
            except (ValueError, IndexError):
                pass
    
    # Extract evacuated
    for pattern in evacuated_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):
                    num = extract_number(match.group(1))
                else:
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["evacuated"] = max(vitimas["evacuated"], num)
            except (ValueError, IndexError):
                pass
    
    # Extract displaced
    for pattern in displaced_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):
                    num = extract_number(match.group(1))
                else:
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["displaced"] = max(vitimas["displaced"], num)
            except (ValueError, IndexError):
                pass
    
    # Extract missing
    for pattern in missing_patterns:
        matches = re.finditer(pattern, title_lower)
        for match in matches:
            try:
                if match.group(1):
                    num = extract_number(match.group(1))
                else:
                    for word, number in num_words.items():
                        if word in match.group(0):
                            num = number
                            break
                    else:
                        continue
                vitimas["missing"] = max(vitimas["missing"], num)
            except (ValueError, IndexError):
                pass
    
    return vitimas

# Add the main entry point at the end of the file
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process relevant articles from scraper output")
    parser.add_argument("--dias", type=int, default=1, help="Number of days to process")
    parser.add_argument("--input_file", type=str, help="Input CSV file path")
    parser.add_argument("--output_dir", type=str, help="Output directory path")
    parser.add_argument("--date_str", type=str, help="Date string for file naming")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.debug:
        logger.setLevel(logging.DEBUG)
        log_progress("🔍 DEBUG logging enabled", "debug")
    
    log_progress("Starting processar_relevantes_airflow")
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
        
        if result:
            log_progress("✅ Processing completed successfully")
            sys.exit(0)
        else:
            log_progress("❌ Processing failed", "error")
            sys.exit(1)
            
    except Exception as e:
        log_progress(f"❌ Processing failed with exception: {e}", "error")
        import traceback
        traceback.print_exc()
        sys.exit(1)