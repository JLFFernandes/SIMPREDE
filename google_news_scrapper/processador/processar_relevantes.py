from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os
import random
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import time
import os
from urllib.parse import urlparse
import re
from extracao.extractor import resolve_google_news_url, fetch_and_extract_article_text
from utils.helpers import carregar_paroquias_com_municipios, load_keywords, carregar_dicofreg, guardar_csv_incremental, detect_municipality 
from extracao.normalizador import detect_disaster_type, extract_victim_counts, normalize, is_potentially_disaster_related
from datetime import datetime, timedelta
import hashlib
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from collections import defaultdict
import argparse

# Use date-specific filenames
current_date = datetime.now().strftime("%Y%m%d")
current_year = datetime.now().strftime("%Y")
current_month = datetime.now().strftime("%m")
current_day = datetime.now().strftime("%d")

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root directory (one level up)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Create directory structure for raw data by year/month/day
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw", current_year, current_month, current_day)
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# Define structured directory
STRUCTURED_DIR = os.path.join(PROJECT_ROOT, "data", "structured")
os.makedirs(STRUCTURED_DIR, exist_ok=True)

# Define input file path
INPUT_CSV = os.path.join(PROJECT_ROOT, "data", "raw", f"intermediate_google_news_{current_date}.csv")

# Define output file paths
OUTPUT_CSV = os.path.join(RAW_DATA_DIR, f"artigos_google_municipios_pt_{current_date}.csv")

# Fallback to default files if date-specific ones don't exist
if not os.path.exists(INPUT_CSV):
    INPUT_CSV = os.path.join(PROJECT_ROOT, "data", "raw", "intermediate_google_news.csv")
    print(f"⚠️ Date-specific input not found, using default: {INPUT_CSV}")
else:
    print(f"✅ Using date-specific input: {INPUT_CSV}")

# Standard output filename for backward compatibility
DEFAULT_OUTPUT_CSV = os.path.join(PROJECT_ROOT, "data", "structured", "artigos_google_municipios_pt.csv")

LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")
FREGUESIAS_COM_CODIGOS = carregar_dicofreg()
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")


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

def processar_artigo(row, rate_limiter):
    original_url = row["link"]
    titulo = row["title"]
    localidade = row["localidade"]
    keyword = row.get("keyword", "desconhecido")
    publicado = row.get("published", "")

    # Initialize the variables before using them
    potential_tipo_from_title = None
    subtipo_from_title = None

    # Check for victims in the title first
    vitimas_do_titulo = extract_victims_from_title(titulo)
    has_victims_in_title = any(vitimas_do_titulo.values())
    if has_victims_in_title:
        print(f"✅ Vítimas encontradas no título: {titulo}")
        # Try to detect disaster type from title
        potential_tipo_from_title, subtipo_from_title = detect_disaster_type(titulo)
        if potential_tipo_from_title != "unknown" and potential_tipo_from_title:
            print(f"🔍 Tipo de desastre identificado do título: {potential_tipo_from_title}")
    
    try:
        resolved_url = resolve_google_news_url(original_url)
        if not resolved_url or not resolved_url.startswith("http"):
            print(f"⚠️ Link não resolvido: {original_url}")
            # If we have victims and disaster type from title, we could still create a partial record
            if has_victims_in_title and potential_tipo_from_title and potential_tipo_from_title != "unknown":
                print(f"💡 Criando registro parcial baseado apenas no título")
                # Create partial record with title information
                # (implement this if you want this feature)
                pass
            return None
    except (requests.RequestException, ConnectionError, TimeoutError) as e:
        print(f"❌ Erro de conexão ao resolver URL {original_url}: {str(e)}")
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
                print(f"⚠️ Artigo ignorado após extração: {titulo}")
                return None
        except (requests.RequestException, ConnectionError, TimeoutError) as e:
            print(f"❌ Erro de conexão ao extrair texto de {resolved_url}: {str(e)}")
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
                print(f"❌ Erro de conexão ao extrair texto de {resolved_url}: {str(e)}")
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
            print(f"🔍 Nenhuma vítima detectada no texto, tentando extrair do título: {titulo}")
            vitimas = vitimas_do_titulo

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
        "missing": vitimas["missing"]
    }

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

def guardar_csv_incremental_with_date(output_csv, artigos):
    """
    Save to date-specific file in the raw data directory with year/month/day structure
    and also maintain backward compatibility with structured directory
    """
    # Using our improved guardar_csv_incremental function which already handles year/month/day organization
    # Save to raw data directory (this will be organized by year/month/day)
    guardar_csv_incremental(output_csv, artigos)
    
    # Also save to standard file for backward compatibility
    guardar_csv_incremental(DEFAULT_OUTPUT_CSV, artigos)
    
    print(f"✅ Files saved with year/month/day organization")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Process Google News articles")
    parser.add_argument("--dias", type=int, default=1, help="Number of days to process")
    args = parser.parse_args()
    
    # Create a shared session for all requests
    session = create_optimized_session()
    rate_limiter = DynamicRateLimiter()

    if not os.path.exists(INPUT_CSV):
        print(f"❌ Erro: O arquivo de entrada '{INPUT_CSV}' não foi encontrado.")
        return

    # Carregar links já importados
    links_existentes = carregar_links_existentes(OUTPUT_CSV)

    try:
        df = pd.read_csv(INPUT_CSV)
        # Filter by days if the collection_date column exists
        if 'collection_date' in df.columns and args.dias > 0:
            cutoff_date = (datetime.now() - timedelta(days=args.dias)).strftime("%Y-%m-%d")
            df = df[df['collection_date'] >= cutoff_date]
            print(f"📅 Filtered to articles from the last {args.dias} days: {len(df)} articles")
        
        relevantes = df.copy()
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
     
        rows = list(relevantes.iloc[start_index:].iterrows())
        artigos_final = []
        max_workers = min(8, os.cpu_count() * 2)  # Dynamic worker count based on CPU cores
        chunk_size = 20  # Process in larger chunks
        
        # Create caches for duplicate detection
        existing_titles = set()
        existing_urls = set()
        
        # Load existing titles from output file to avoid duplicates
        if os.path.exists(OUTPUT_CSV):
            try:
                df_existing = pd.read_csv(OUTPUT_CSV)
                # Populate caches from existing data
                for title in df_existing.get("title", []):
                    if isinstance(title, str):
                        existing_titles.add(hashlib.md5(title.lower().encode()).hexdigest())
                for url in df_existing.get("page", []):
                    if isinstance(url, str):
                        existing_urls.add(url.split('?')[0])
            except Exception as e:
                print(f"⚠️ Erro ao carregar títulos existentes: {e}")
        
        # Process articles in batches to avoid overloading
        for chunk_start in range(start_index, len(rows), chunk_size):
            chunk_end = min(chunk_start + chunk_size, len(rows))
            chunk_rows = rows[chunk_start:chunk_end]
            
            # Random delay between chunks to appear more human-like
            if chunk_start > start_index:
                delay = random.uniform(5, 15)
                print(f"🕒 Pausa entre batches: {delay:.1f} segundos...")
                time.sleep(delay)
            
            # Check internet connection before processing batch
            if not check_internet_connection():
                print("⚠️ Conexão com a internet perdida. Aguardando reconexão...")
                while not check_internet_connection():
                    time.sleep(30)  # Wait 30 seconds before checking again
                print("✅ Conexão com a internet restaurada. Continuando processamento...")
            
            batch_articles = []
            try:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_row = {executor.submit(processar_artigo, row[1], rate_limiter): row for row in chunk_rows}
                    for future in as_completed(future_to_row):
                        try:
                            artigo = future.result()
                            if artigo and artigo["page"] not in links_existentes:
                                batch_articles.append(artigo)
                                links_existentes.add(artigo["page"])
                        except Exception as e:
                            print(f"❌ Erro ao processar artigo: {str(e)}")
                            continue
            except Exception as e:
                print(f"❌ Erro durante o processamento do lote: {str(e)}")
                # Save what we have so far
                if batch_articles:
                    try:
                        guardar_csv_incremental(OUTPUT_CSV, batch_articles)
                        artigos_final.extend(batch_articles)
                        print(f"✅ Salvamento de emergência: {len(batch_articles)} artigos após erro.")
                    except Exception as save_err:
                        print(f"❌ Erro ao salvar após falha: {str(save_err)}")
                time.sleep(60)  # Wait a minute before continuing
                continue
            
            # Save after each batch
            if batch_articles:
                try:
                    guardar_csv_incremental_with_date(OUTPUT_CSV, batch_articles)
                    artigos_final.extend(batch_articles)
                    print(f"✅ Batch concluído: {len(batch_articles)} artigos (total: {len(artigos_final)})")
                    time.sleep(random.uniform(3, 5))  # Short break after each batch
                except Exception as e:
                    print(f"❌ Erro ao salvar batch: {str(e)}")
                    time.sleep(10)  # Wait a bit and try again
                    try:
                        guardar_csv_incremental_with_date(OUTPUT_CSV, batch_articles)
                        print("✅ Segundo tentativa de salvamento bem-sucedida.")
                    except:
                        print("❌ Falha na segunda tentativa de salvamento.")

        if artigos_final:
            guardar_csv_incremental_with_date(OUTPUT_CSV, artigos_final)
            print(f"✅ Base de dados final atualizada com {len(artigos_final)} artigos.")
            print(f"✅ Arquivos salvos em: {OUTPUT_CSV} e {DEFAULT_OUTPUT_CSV}")
        else:
            print("⚠️ Nenhum artigo foi processado com sucesso.")
    
    except KeyboardInterrupt:
        print("\n⚠️ Interrompido pelo usuário. Salvando artigos processados até agora...")
        if 'artigos_final' in locals() and artigos_final:
            try:
                guardar_csv_incremental(OUTPUT_CSV, artigos_final)
                print(f"✅ Salvamento de emergência: {len(artigos_final)} artigos salvos.")
            except Exception as e:
                print(f"❌ Erro ao salvar durante interrupção: {str(e)}")
    
    except Exception as e:
        print(f"❌ Erro inesperado: {str(e)}")
        if 'artigos_final' in locals() and artigos_final:
            try:
                guardar_csv_incremental(OUTPUT_CSV, artigos_final)
                print(f"✅ Salvamento de emergência: {len(artigos_final)} artigos salvos.")
            except Exception as save_err:
                print(f"❌ Erro ao salvar durante erro: {str(save_err)}")

if __name__ == "__main__":
    main()