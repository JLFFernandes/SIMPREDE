#!/usr/bin/env python3
# filepath: /Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow/dags/google_scraper/main_airflow.py
"""
Airflow-friendly main script to orchestrate scraping, processing, and exporting.
This script serves as a reference for how the pipeline is organized when using Airflow.
It doesn't need to be run directly as Airflow will execute each step separately.
"""
import argparse
from datetime import datetime
import sys
import os

def print_airflow_usage_instructions():
    """Print instructions for using the pipeline with Airflow"""
    print("""
🚀 Pipeline de Scraping Google com Airflow 🚀

Este pipeline está configurado para ser executado com o Apache Airflow. Em vez de executar este script,
deverá acionar o DAG através da interface web do Airflow ou da linha de comandos.

O pipeline consiste nas seguintes tarefas:
1. run_scraper - Recolhe artigos de notícias do Google News
2. processar_relevantes - Processa artigos para determinar a relevância
3. filtrar_artigos_vitimas - Filtra artigos relacionados com vítimas de desastres
4. export_to_supabase - Exporta os artigos filtrados para a base de dados

Para executar o pipeline via linha de comandos do Airflow:
  airflow dags trigger google_scraper_pipeline --conf '{"dias": 3, "date": "2025-06-01"}'

Para executar via interface web do Airflow:
1. Aceda ao painel do Airflow (normalmente em http://localhost:8080)
2. Localize o DAG 'google_scraper_pipeline'
3. Clique em "Trigger DAG" e adicione opcionalmente parâmetros de configuração:
   {
     "dias": 3,
     "date": "2025-06-01"
   }

Parâmetros:
- dias: Número de dias anteriores a considerar para as notícias (predefinição: 1)
- date: Data específica para processar no formato AAAA-MM-DD (opcional)

O pipeline será executado automaticamente diariamente de acordo com a programação na configuração do DAG.
    """)

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Pipeline de scraping e processamento de notícias (Airflow)")
    parser.add_argument(
        "--info",
        action="store_true",
        help="Display information about using the pipeline with Airflow"
    )
    
    args = parser.parse_args()
    
    # Print Airflow usage instructions
    print_airflow_usage_instructions()
    
    print("\n⚠️ Nota: Este script serve apenas como referência. O pipeline real é executado pelo Airflow.")
    print("✅ Para ver a configuração do DAG, verifique: dags/google_scraper_dag.py")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
