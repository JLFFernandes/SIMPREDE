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
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import urlparse

# Set unbuffered output at the very beginning - FIXED VERSION
os.environ['PYTHONUNBUFFERED'] = '1'
# Force stdout to be unbuffered using reconfigure (Python 3.7+)
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

def extract_victims_from_title(title):
    """
    Extrai contagens de vítimas do título da notícia
    Retorna um dicionário com as contagens encontradas
    """
    vitimas = {
        "fatalities": 0,
        "injured": 0,
        "evacuated": 0,
        "displaced": 0,
        "missing": 0
    }
    
    # Padrões para mortos/vítimas mortais
    fatalities_patterns = [
        r'(\d+)\s*mort[eo]s?',
        r'(\d+)\s*vítimas?\s*mortais?',
        r'(\d+)\s*óbitos?',
        r'faz\s+(\d+)\s+mortos?',  # "faz três mortos"
        r'deixa\s+(\d+)\s+mortos?',  # "deixa quatro mortos"
        r'matou\s+(\d+)',  # "matou cinco"
        r'morrem\s+(\d+)'   # "morrem seis"
    ]
    
    # Padrões para feridos
    injured_patterns = [
        r'(\d+)\s*feridos?',
        r'(\d+)\s*pessoas?\s*feridas?',
        r'deixa\s+(\d+)\s+feridos?'
    ]
    
    # Padrões para evacuados
    evacuated_patterns = [
        r'(\d+)\s*evacuad[oa]s?',
        r'(\d+)\s*pessoas?\s*evacuadas?'
    ]
    
    # Padrões para desalojados
    displaced_patterns = [
        r'(\d+)\s*desalojad[oa]s?',
        r'(\d+)\s*pessoas?\s*desalojadas?'
    ]
    
    # Padrões para desaparecidos
    missing_patterns = [
        r'(\d+)\s*desaparecid[oa]s?',
        r'(\d+)\s*pessoas?\s*desaparecidas?',
        r'deixa\s+(\d+)\s+desaparecid[oa]s?',
        r'e\s+(\d+)\s+desaparecid[oa]'  # "e um desaparecido"
    ]

    # Procurar por cada tipo de vítima
    for pattern in fatalities_patterns:
        match = re.search(pattern, title.lower())
        if match:
            try:
                vitimas["fatalities"] = int(match.group(1))
            except ValueError:
                # Converter palavras para números, se necessário
                num_str = match.group(1).lower()
                num_map = {"um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "tres": 3, 
                          "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8}
                if num_str in num_map:
                    vitimas["fatalities"] = num_map[num_str]
            break

    for pattern in injured_patterns:
        match = re.search(pattern, title.lower())
        if match:
            try:
                vitimas["injured"] = int(match.group(1))
            except ValueError:
                num_str = match.group(1).lower()
                num_map = {"um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "tres": 3, 
                          "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8}
                if num_str in num_map:
                    vitimas["injured"] = num_map[num_str]
            break

    for pattern in evacuated_patterns:
        match = re.search(pattern, title.lower())
        if match:
            try:
                vitimas["evacuated"] = int(match.group(1))
            except ValueError:
                num_str = match.group(1).lower()
                num_map = {"um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "tres": 3, 
                          "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8}
                if num_str in num_map:
                    vitimas["evacuated"] = num_map[num_str]
            break

    for pattern in displaced_patterns:
        match = re.search(pattern, title.lower())
        if match:
            try:
                vitimas["displaced"] = int(match.group(1))
            except ValueError:
                num_str = match.group(1).lower()
                num_map = {"um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "tres": 3, 
                          "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8}
                if num_str in num_map:
                    vitimas["displaced"] = num_map[num_str]
            break

    for pattern in missing_patterns:
        match = re.search(pattern, title.lower())
        if match:
            try:
                vitimas["missing"] = int(match.group(1))
            except ValueError:
                num_str = match.group(1).lower()
                num_map = {"um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "tres": 3, 
                          "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8}
                if num_str in num_map:
                    vitimas["missing"] = num_map[num_str]
            break
    
    return vitimas

def processar_artigo(row, rate_limiter, LOCALIDADES, KEYWORDS, FREGUESIAS_COM_CODIGOS, progress_callback=None):
    """Process a single article with progress callback"""
    # Extract data from the row with exact field names from scraper output
    original_url = row.get("link", "")
    titulo = row.get("title", "")
    localidade = row.get("localidade", "")
    keyword = row.get("keyword", "desconhecido")
    publicado = row.get("published", "")
    article_id = row.get("ID", "")
    
    # Validate required fields
    if not all([original_url, titulo, article_id]):
        log_progress(f"⚠️ Artigo com dados incompletos ignorado: ID={article_id}, título={titulo[:30]}...", "warning")
        return None

    # Initialize the variables before using them
    potential_tipo_from_title = None
    subtipo_from_title = None

    # Check for victims in the title first
    vitimas_do_titulo = extract_victims_from_title(titulo)
    has_victims_in_title = any(vitimas_do_titulo.values())
    if has_victims_in_title:
        log_progress(f"✅ Vítimas encontradas no título: {titulo}")
        # Try to detect disaster type from title
        potential_tipo_from_title, subtipo_from_title = detect_disaster_type(titulo)
        if potential_tipo_from_title != "unknown" and potential_tipo_from_title:
            log_progress(f"🔍 Tipo de desastre identificado do título: {potential_tipo_from_title}")
    
    try:
        resolved_url = resolve_google_news_url(original_url)
        if not resolved_url or not resolved_url.startswith("http"):
            log_progress(f"⚠️ Link não resolvido: {original_url}", "warning")
            # If we have victims and disaster type from title, we could still create a partial record
            if has_victims_in_title and potential_tipo_from_title and potential_tipo_from_title != "unknown":
                log_progress(f"💡 Criando registro parcial baseado apenas no título")
                # Create partial record with title information
                # (implement this if you want this feature)
                pass
            return None
    except (requests.RequestException, ConnectionError, TimeoutError) as e:
        log_progress(f"❌ Erro de conexão ao resolver URL {original_url}: {str(e)}", "error")
        # Wait a bit before continuing
        time.sleep(5)
        return None

    # Only fetch full text if needed
    texto = ""
    if not has_victims_in_title or not potential_tipo_from_title or potential_tipo_from_title == "unknown":
        try:
            rate_limiter.wait_if_needed(resolved_url)  # Wait before the request
            texto = fetch_and_extract_article_text(resolved_url)
            if not texto or not is_potentially_disaster_related(texto, KEYWORDS):
                log_progress(f"⚠️ Artigo ignorado após extração: {titulo}", "warning")
                return None
        except (requests.RequestException, ConnectionError, TimeoutError) as e:
            log_progress(f"❌ Erro de conexão ao extrair texto de {resolved_url}: {str(e)}", "error")
            # Wait a bit before continuing
            time.sleep(5)
            return None
    
    # Use title-based disaster type if available, otherwise extract from text
    if potential_tipo_from_title and potential_tipo_from_title != "unknown":
        tipo, subtipo = potential_tipo_from_title, subtipo_from_title
    else:
        if not texto:  # Fetch text if we haven't already
            try:
                rate_limiter.wait_if_needed(resolved_url)  # Wait before the request
                texto = fetch_and_extract_article_text(resolved_url)
            except (requests.RequestException, ConnectionError, TimeoutError) as e:
                log_progress(f"❌ Erro de conexão ao extrair texto de {resolved_url}: {str(e)}", "error")
                # Wait a bit before continuing
                time.sleep(5)
                return None
        tipo, subtipo = detect_disaster_type(texto)
    
    # Use title victims if they exist, otherwise extract from text
    if has_victims_in_title:
        vitimas = vitimas_do_titulo
    else:
        vitimas = extract_victim_counts(texto)
        # If no victims found in text, try again with title as backup
        if not any(vitimas.values()):
            log_progress(f"🔍 Nenhuma vítima detectada no texto, tentando extrair do título: {titulo}")
            vitimas = vitimas_do_titulo

    loc = detect_municipality(texto, LOCALIDADES) or localidade
    district = LOCALIDADES.get(loc.lower(), {}).get("district", "")
    concelho = LOCALIDADES.get(loc.lower(), {}).get("municipality", "")
    parish_normalized = normalize(loc.lower())
    dicofreg = FREGUESIAS_COM_CODIGOS.get(parish_normalized, "")

    data_evt_formatada, ano, mes, dia, hora_evt = formatar_data_para_ddmmyyyy(publicado)
    fonte = extrair_nome_fonte(resolved_url)

    # Use the ID from the scraper output
    if not article_id:
        log_progress(f"⚠️ Artigo sem ID válido ignorado: {titulo[:30]}...", "warning")
        return None

    result = {
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
        "missing": vitimas["missing"]
    }

    if progress_callback:
        progress_callback(f"Processado: {titulo[:50]}...")

    return result

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
            import json
            with open(paths['stats_json'], 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            log_progress(f"✅ Processing statistics saved: {paths['stats_json']}")
        except Exception as e:
            log_progress(f"⚠️ Could not save statistics: {e}", "warning")

# Add a progress callback function
def progress_update(message):
    """Progress callback for article processing"""
    log_progress(f"🔄 {message}")

def airflow_main(target_date=None, dias=1, input_file=None, output_dir=None, date_str=None):
    """
    Main function optimized for Airflow execution with controlled paths
    Returns number of articles processed for Airflow compatibility
    """
    log_progress("🚀 A iniciar processar_relevantes_airflow")
    
    # Setup paths and configuration with controlled paths
    try:
        paths = setup_paths_and_dates(target_date, dias, input_file, output_dir, date_str)
        if not paths['input_csv']:
            log_progress("❌ Nenhum ficheiro de entrada encontrado. Não é possível prosseguir.", "error")
            return 0
        
        LOCALIDADES, MUNICIPIOS, DISTRITOS, KEYWORDS, FREGUESIAS_COM_CODIGOS = load_configuration()
        
    except Exception as e:
        log_progress(f"❌ Configuração falhada: {e}", "error")
        raise
    
    # Create a shared session for all requests
    session = create_optimized_session()
    rate_limiter = DynamicRateLimiter()

    # Carregar links já importados
    links_existentes = carregar_links_existentes(paths['output_csv'])

    try:
        df = pd.read_csv(paths['input_csv'])
        
        # Validate CSV format matches expectations
        required_columns = ["ID", "keyword", "localidade", "title", "link", "published"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            log_progress(f"❌ Colunas em falta no ficheiro CSV: {missing_columns}", "error")
            log_progress(f"🔍 Colunas disponíveis: {list(df.columns)}", "debug")
            return 0
        
        log_progress(f"✅ Ficheiro CSV válido com {len(df)} artigos")
        log_progress(f"📋 Colunas: {list(df.columns)}")
        
        # Filter by days if the collection_date column exists
        if 'collection_date' in df.columns and dias > 0:
            cutoff_date = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
            df = df[df['collection_date'] >= cutoff_date]
            log_progress(f"📅 Filtrado para artigos dos últimos {dias} dias: {len(df)} artigos")
        
        relevantes = df.copy()
        processing_stats = {
            "Total de Artigos para Processar": len(relevantes),
            "Ficheiro de Entrada": paths['input_csv'],
            "Ficheiro de Saída Principal": paths['default_output_csv'],
            "Diretoria de Saída": paths['structured_dir'],
            "Filtro de Dias": dias
        }
        log_statistics(processing_stats, "Processamento Iniciado")
        
        # Tentar identificar o último ID processado
        df_existente = pd.read_csv(paths['output_csv']) if os.path.exists(paths['output_csv']) else pd.DataFrame()
        ultimo_id = df_existente["ID"].dropna().iloc[-1] if not df_existente.empty else None
     
        # Obter a posição do último ID no DataFrame de relevantes
        start_index = 0
        if ultimo_id:
            try:
                start_index = relevantes[relevantes["ID"] == ultimo_id].index[-1] + 1
                log_progress(f"⏩ A retomar do índice {start_index} após o ID: {ultimo_id}")
            except IndexError:
                log_progress("⚠️ Último ID não encontrado em relevantes. A começar do início.", "warning")
     
        rows = list(relevantes.iloc[start_index:].iterrows())
        artigos_final = []
        max_workers = min(8, os.cpu_count() * 2) if os.cpu_count() else 4  # Dynamic worker count based on CPU cores
        chunk_size = 20  # Process in larger chunks
        
        # Create caches for duplicate detection
        existing_titles = set()
        existing_urls = set()
        
        # Load existing titles from output file to avoid duplicates
        if os.path.exists(paths['output_csv']):
            try:
                df_existing = pd.read_csv(paths['output_csv'])
                # Populate caches from existing data
                for title in df_existing.get("title", []):
                    if isinstance(title, str):
                        existing_titles.add(hashlib.md5(title.lower().encode()).hexdigest())
                for url in df_existing.get("page", []):
                    if isinstance(url, str):
                        existing_urls.add(url.split('?')[0])
            except Exception as e:
                log_progress(f"⚠️ Erro ao carregar títulos existentes: {e}", "warning")
        
        # Process articles in batches to avoid overloading
        total_batches = (len(rows) + chunk_size - 1) // chunk_size
        for chunk_start in range(0, len(rows), chunk_size):
            chunk_end = min(chunk_start + chunk_size, len(rows))
            chunk_rows = rows[chunk_start:chunk_end]
            batch_num = (chunk_start // chunk_size) + 1
            
            log_progress(f"📦 A processar lote {batch_num}/{total_batches} ({len(chunk_rows)} artigos)")
            
            # Random delay between chunks to appear more human-like
            if chunk_start > 0:
                delay = random.uniform(5, 15)
                log_progress(f"🕒 Pausa entre lotes: {delay:.1f} segundos...")
                time.sleep(delay)
            
            # Check internet connection before processing batch
            if not check_internet_connection():
                log_progress("⚠️ Conexão com a internet perdida. A aguardar reconexão...", "warning")
                while not check_internet_connection():
                    time.sleep(30)  # Wait 30 seconds before checking again
                log_progress("✅ Conexão com a internet restaurada. A continuar processamento...")
            
            batch_articles = []
            try:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_row = {
                        executor.submit(processar_artigo, row[1], rate_limiter, LOCALIDADES, KEYWORDS, FREGUESIAS_COM_CODIGOS, progress_update): row 
                        for row in chunk_rows
                    }
                    for future in as_completed(future_to_row):
                        try:
                            artigo = future.result()
                            if artigo and artigo["page"] not in links_existentes:
                                batch_articles.append(artigo)
                                links_existentes.add(artigo["page"])
                        except Exception as e:
                            log_progress(f"❌ Erro ao processar artigo: {str(e)}", "error")
                            continue
            except Exception as e:
                log_progress(f"❌ Erro durante o processamento do lote: {str(e)}", "error")
                # Save what we have so far
                if batch_articles:
                    try:
                        guardar_csv_incremental(paths['output_csv'], batch_articles)
                        artigos_final.extend(batch_articles)
                        log_progress(f"✅ Guardamento de emergência: {len(batch_articles)} artigos após erro.")
                    except Exception as save_err:
                        log_progress(f"❌ Erro ao guardar após falha: {str(save_err)}", "error")
                time.sleep(60)  # Wait a minute before continuing
                continue
            
            # Save after each batch
            if batch_articles:
                try:
                    guardar_csv_incremental_with_controlled_paths(paths, batch_articles)
                    artigos_final.extend(batch_articles)
                    log_progress(f"✅ Lote {batch_num} concluído: {len(batch_articles)} artigos (total: {len(artigos_final)})")
                    time.sleep(random.uniform(3, 5))  # Short break after each batch
                except Exception as e:
                    log_progress(f"❌ Erro ao guardar lote: {str(e)}", "error")
                    time.sleep(10)  # Wait a bit and try again
                    try:
                        guardar_csv_incremental_with_controlled_paths(paths, batch_articles)
                        log_progress("✅ Segunda tentativa de guardamento bem-sucedida.")
                    except:
                        log_progress("❌ Falha na segunda tentativa de guardamento.", "error")

        final_stats = {
            "Total de Artigos Processados": len(artigos_final),
            "Ficheiro de Saída Principal": paths['default_output_csv'],
            "Ficheiro de Saída Raw": paths['output_csv'],
            "Ficheiro de Estatísticas": paths['stats_json'],
            "Taxa de Sucesso": f"{len(artigos_final)/len(relevantes)*100:.1f}%" if len(relevantes) > 0 else "0%"
        }
        
        if artigos_final:
            guardar_csv_incremental_with_controlled_paths(paths, artigos_final)
            log_statistics(final_stats, "Processamento Concluído com Sucesso")
            return len(artigos_final)
        else:
            log_progress("⚠️ Nenhum artigo foi processado com sucesso.", "warning")
            return 0
    
    except KeyboardInterrupt:
        log_progress("\n⚠️ Interrompido pelo utilizador. A guardar artigos processados até agora...", "warning")
        if 'artigos_final' in locals() and artigos_final:
            try:
                guardar_csv_incremental(paths['output_csv'], artigos_final)
                log_progress(f"✅ Guardamento de emergência: {len(artigos_final)} artigos guardados.")
                return len(artigos_final)
            except Exception as e:
                log_progress(f"❌ Erro ao guardar durante interrupção: {str(e)}", "error")
                return 0
    
    except Exception as e:
        log_progress(f"❌ Erro inesperado: {str(e)}", "error")
        if 'artigos_final' in locals() and artigos_final:
            try:
                guardar_csv_incremental(paths['output_csv'], artigos_final)
                log_progress(f"✅ Guardamento de emergência: {len(artigos_final)} artigos guardados.")
                return len(artigos_final)
            except Exception as save_err:
                log_progress(f"❌ Erro ao guardar durante erro: {str(save_err)}", "error")
                return 0

def main():
    """Main function with controlled output paths support"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Process Google News articles (Airflow version)")
    parser.add_argument("--dias", type=int, default=1, help="Number of days to process")
    parser.add_argument("--date", type=str, help="Specific target date (YYYY-MM-DD)")
    parser.add_argument("--input_file", type=str, help="Specific input file path")
    parser.add_argument("--output_dir", type=str, help="Output directory for processed files")
    parser.add_argument("--date_str", type=str, help="Date string for file naming")
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
        log_progress(f"✅ Processing completed. Processed {result} articles")
        return 0
    except Exception as e:
        log_progress(f"❌ Processing failed: {e}", "error")
        return 1

if __name__ == "__main__":
    sys.exit(main())