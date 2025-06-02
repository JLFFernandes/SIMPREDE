#!/usr/bin/env python3
# filepath: /Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow/dags/google_scraper/processador/filtrar_artigos_vitimas_airflow.py
"""
Versão compatível com Airflow do filtrar_artigos_vitimas.py
Modificações:
- Removidas entradas interativas
- Adicionado melhor logging para Airflow
- Melhorado o tratamento de erros
- Adaptado para lidar corretamente com o contexto do Airflow
"""
import pandas as pd
import json
import re
import os
import sys
import logging
import argparse
from datetime import datetime
from typing import Dict, Optional

# Add parent directory to path to allow importing utils
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("filtrar_artigos_vitimas_airflow")

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
    structured_dir = os.path.join(PROJECT_ROOT, "data", "structured", current_year, current_month, current_day)
    
    # Ensure directories exist
    os.makedirs(raw_data_dir, exist_ok=True)
    os.makedirs(structured_dir, exist_ok=True)
    
    # Input file
    relevant_articles_csv = os.path.join(raw_data_dir, f"relevant_articles_{current_date}.csv")
    
    # Output files
    victim_articles_csv = os.path.join(structured_dir, f"victim_articles_{current_date}.csv")
    all_events_csv = os.path.join(structured_dir, f"all_events_{current_date}.csv")
    
    return (
        raw_data_dir, 
        structured_dir, 
        relevant_articles_csv, 
        victim_articles_csv, 
        all_events_csv, 
        current_date
    )

def has_victim_info(row: Dict) -> bool:
    """Check if an article has victim information"""
    # Check if any victim field has a value
    return (
        row.get('fatalities', 0) > 0 or 
        row.get('injuries', 0) > 0 or 
        row.get('missing', 0) > 0 or
        row.get('affected', 0) > 0
    )

def detect_event_type(disaster_type: str) -> str:
    """Classify the disaster type into a broader event category"""
    disaster_type = disaster_type.lower()
    
    if any(term in disaster_type for term in ['inundação', 'inundacoes', 'cheia', 'chuva']):
        return 'inundação'
    elif any(term in disaster_type for term in ['incêndio', 'incendio', 'fogo']):
        return 'incêndio'
    elif 'sismo' in disaster_type or 'terremoto' in disaster_type:
        return 'sismo'
    elif any(term in disaster_type for term in ['deslizamento', 'derrocada', 'desabamento']):
        return 'deslizamento_terra'
    elif 'tempestade' in disaster_type or 'furacão' in disaster_type or 'tornado' in disaster_type:
        return 'tempestade'
    elif 'seca' in disaster_type:
        return 'seca'
    elif 'calor' in disaster_type or 'onda de calor' in disaster_type:
        return 'onda_calor'
    elif 'neve' in disaster_type or 'gelo' in disaster_type:
        return 'neve_gelo'
    else:
        return 'outro'

def standardize_data(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize data to ensure consistent format"""
    # Convert numeric columns to integers with safe handling of NaN
    for col in ['fatalities', 'injuries', 'missing', 'affected']:
        df[col] = df[col].fillna(0).astype(int)
    
    # Add event type column
    df['event_type'] = df['disaster_type'].apply(detect_event_type)
    
    # Add timestamp
    df['processed_timestamp'] = datetime.now().isoformat()
    
    return df

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Filtra artigos com informação sobre vítimas (versão Airflow)")
    parser.add_argument("--dias", type=int, default=1, help="Número de dias anteriores a considerar")
    parser.add_argument("--date", type=str, help="Data específica para processar (AAAA-MM-DD)")
    
    args = parser.parse_args()
    
    logger.info("A iniciar a filtragem de artigos com vítimas (versão Airflow)")
    logger.info(f"Dias anteriores: {args.dias}, Data específica: {args.date}")
    
    # Get appropriate paths
    (
        raw_data_dir, 
        structured_dir, 
        relevant_articles_csv, 
        victim_articles_csv, 
        all_events_csv, 
        current_date
    ) = get_date_paths(args.date)
    
    # Check if relevant articles CSV exists
    if not os.path.exists(relevant_articles_csv):
        logger.error(f"CSV de artigos relevantes não encontrado: {relevant_articles_csv}")
        sys.exit(1)
    
    # Read relevant articles CSV
    try:
        df = pd.read_csv(relevant_articles_csv)
        logger.info(f"Carregados {len(df)} artigos relevantes de {relevant_articles_csv}")
    except Exception as e:
        logger.error(f"Erro ao ler o CSV: {e}")
        sys.exit(1)
    
    # Standardize data
    df = standardize_data(df)
    
    # Filter articles with victim information
    df_victims = df[df.apply(has_victim_info, axis=1)]
    logger.info(f"Encontrados {len(df_victims)} artigos com informação sobre vítimas")
    
    # Save victim articles
    df_victims.to_csv(victim_articles_csv, index=False)
    logger.info(f"Guardados {len(df_victims)} artigos com vítimas em {victim_articles_csv}")
    
    # Save all events (for reference)
    df.to_csv(all_events_csv, index=False)
    logger.info(f"Guardados {len(df)} eventos totais em {all_events_csv}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
