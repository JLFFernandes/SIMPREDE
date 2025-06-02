#!/usr/bin/env python3
# filepath: /Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow/dags/google_scraper/processador/processar_relevantes_airflow.py
"""
Versão compatível com Airflow do processar_relevantes.py
Modificações:
- Removidas entradas interativas
- Adicionado melhor logging para Airflow
- Melhorado o tratamento de erros
- Adaptado para lidar corretamente com o contexto do Airflow
"""
import sys
import os
import logging
import pandas as pd
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from urllib.parse import urlparse
import re
import hashlib
from collections import defaultdict

# Add parent directory to path to allow importing utils
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

# Import project modules with error handling
try:
    from extracao.extractor import resolve_google_news_url, fetch_and_extract_article_text
    from utils.helpers import carregar_paroquias_com_municipios, load_keywords, carregar_dicofreg, guardar_csv_incremental, detect_municipality
    from extracao.normalizador import detect_disaster_type, extract_victim_counts, normalize, is_potentially_disaster_related
except ImportError as e:
    print(f"Error importing required modules: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("processar_relevantes_airflow")

# Setup HTTP retry functionality
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import requests
    
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("http://", adapter)
    http.mount("https://", adapter)
except ImportError:
    logger.error("Erro ao importar bibliotecas HTTP. A instalar pacotes necessários...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "urllib3"])
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import requests
    
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("http://", adapter)
    http.mount("https://", adapter)

def get_date_paths(specified_date=None):
    """Get appropriate date paths based on the specified date or current date"""
    if specified_date:
        try:
            dt = datetime.strptime(specified_date, "%Y-%m-%d")
            current_date = dt.strftime("%Y%m%d")
            current_year = dt.strftime("%Y")
            current_month = dt.strftime("%m")
            current_day = dt.strftime("%d")
        except ValueError as e:
            logger.error(f"Formato de data inválido: {e}")
            raise
    else:
        # Use current date
        dt = datetime.now()
        current_date = dt.strftime("%Y%m%d")
        current_year = dt.strftime("%Y")
        current_month = dt.strftime("%m")
        current_day = dt.strftime("%d")
    
    # Define paths with year/month/day structure
    raw_data_dir = os.path.join(PROJECT_ROOT, "data", "raw", current_year, current_month, current_day)
    intermediate_csv = os.path.join(raw_data_dir, f"intermediate_google_news_{current_date}.csv")
    relevant_articles_csv = os.path.join(raw_data_dir, f"relevant_articles_{current_date}.csv")
    
    # Create directory if it doesn't exist
    os.makedirs(raw_data_dir, exist_ok=True)
    
    # For backwards compatibility
    default_intermediate_csv = os.path.join(PROJECT_ROOT, "data", "raw", "intermediate_google_news.csv")
    
    return raw_data_dir, intermediate_csv, relevant_articles_csv, default_intermediate_csv, current_date

def process_article(article, keywords, localidades, municipios, distritos):
    """Process a single article"""
    try:
        article_id = article['id']
        url = article['link']
        title = article['title']
        source = article['source']
        
        # Resolve Google News redirect to get the actual article URL
        resolved_url = resolve_google_news_url(url)
        if not resolved_url:
            logger.warning(f"Falha ao resolver o URL: {url}")
            return None
        
        # Extract article text
        article_text = fetch_and_extract_article_text(resolved_url)
        if not article_text or len(article_text) < 50:
            logger.warning(f"Texto insuficiente extraído de {resolved_url}")
            return None
        
        # Normalize text
        normalized_text = normalize(article_text)
        
        # Check if potentially disaster related
        if not is_potentially_disaster_related(normalized_text, title, keywords):
            return None
        
        # Detect disaster type
        disaster_type = detect_disaster_type(normalized_text, title)
        if not disaster_type:
            return None
        
        # Extract victim counts
        fatalities, injuries, missing, affected = extract_victim_counts(normalized_text)
        
        # Detect municipality
        municipio = detect_municipality(normalized_text, localidades, municipios, distritos)
        
        # Parse domain from URL
        parsed_url = urlparse(resolved_url)
        domain = parsed_url.netloc
        
        # Create result
        result = {
            'id': article_id,
            'title': title,
            'original_url': url,
            'resolved_url': resolved_url,
            'domain': domain,
            'source': source,
            'text': article_text,
            'disaster_type': disaster_type,
            'fatalities': fatalities,
            'injuries': injuries,
            'missing': missing,
            'affected': affected,
            'municipio': municipio,
            'processed_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return result
    except Exception as e:
        logger.error(f"Erro ao processar artigo {article.get('id', 'unknown')}: {e}")
        return None

def process_articles(df, max_workers=10):
    """Process articles in parallel"""
    # Load necessary data
    keywords = load_keywords("config/keywords.json", idioma="portuguese")
    localidades, municipios, distritos = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")
    
    results = []
    total = len(df)
    processed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_article = {
            executor.submit(process_article, article.to_dict(), keywords, localidades, municipios, distritos): article
            for _, article in df.iterrows()
        }
        
        # Process results as they complete
        for future in as_completed(future_to_article):
            article = future_to_article[future]
            processed += 1
            
            if processed % 10 == 0 or processed == total:
                logger.info(f"Processados {processed}/{total} artigos")
            
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                logger.error(f"Erro ao processar artigo {article.get('id', 'unknown')}: {e}")
    
    return results

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Processa artigos para relevância (versão Airflow)")
    parser.add_argument("--dias", type=int, default=1, help="Número de dias anteriores a considerar")
    parser.add_argument("--date", type=str, help="Data específica para processar (AAAA-MM-DD)")
    
    args = parser.parse_args()
    
    logger.info("A iniciar o processamento de artigos (versão Airflow)")
    logger.info(f"Dias anteriores: {args.dias}, Data específica: {args.date}")
    
    # Get appropriate paths
    raw_data_dir, intermediate_csv, relevant_articles_csv, default_intermediate_csv, current_date = get_date_paths(args.date)
    
    # Check if intermediate CSV exists, use default if not
    if not os.path.exists(intermediate_csv) and os.path.exists(default_intermediate_csv):
        logger.info(f"A usar o CSV intermédio predefinido: {default_intermediate_csv}")
        intermediate_csv = default_intermediate_csv
    
    # Check if file exists
    if not os.path.exists(intermediate_csv):
        logger.error(f"CSV intermédio não encontrado: {intermediate_csv}")
        sys.exit(1)
    
    # Read intermediate CSV
    try:
        df = pd.read_csv(intermediate_csv)
        logger.info(f"Carregados {len(df)} artigos de {intermediate_csv}")
    except Exception as e:
        logger.error(f"Erro ao ler o CSV: {e}")
        sys.exit(1)
    
    # Filter unprocessed articles
    df_unprocessed = df[df['processed'] == False]
    logger.info(f"Encontrados {len(df_unprocessed)} artigos não processados")
    
    if len(df_unprocessed) == 0:
        logger.info("Sem artigos para processar, a terminar")
        sys.exit(0)
    
    # Process articles
    results = process_articles(df_unprocessed)
    logger.info(f"Processados {len(results)} artigos relevantes")
    
    # Save results
    if results:
        df_results = pd.DataFrame(results)
        df_results.to_csv(relevant_articles_csv, index=False)
        logger.info(f"Guardados {len(results)} artigos relevantes em {relevant_articles_csv}")
    
    # Mark articles as processed in the intermediate CSV
    df.loc[df['id'].isin(df_unprocessed['id']), 'processed'] = True
    df.to_csv(intermediate_csv, index=False)
    logger.info(f"Atualizados {len(df_unprocessed)} artigos como processados em {intermediate_csv}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
