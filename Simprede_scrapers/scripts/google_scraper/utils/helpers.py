"""
Utilidades e funções auxiliares para normalização de texto, carregamento de dados e exportação
Preparado para produção com comentários em português
"""

import unicodedata
import pandas as pd
import re
from datetime import datetime
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
import json
import csv
import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse, parse_qs, urlencode
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import logging

# Local normalize function to avoid circular imports
def normalize(text):
    """Normalize text for comparison"""
    if text is None:
        return ""
    return unicodedata.normalize('NFKD', str(text).lower()).encode('ASCII', 'ignore').decode('utf-8').strip()

# Import normalize from local definition instead of normalizador
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse
import hashlib

# Setup logging
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# --------------------------- Carregamento do mapa de localizações ---------------------------
with open(os.path.join(PROJECT_ROOT, "config/municipios_por_distrito.json"), "r", encoding="utf-8") as f:
    raw_data = json.load(f)

MAPA_LOCALIZACOES = []
for provincia, distritos in raw_data.items():
    for distrito, postos in distritos.items():
        for posto in postos:
            MAPA_LOCALIZACOES.append({
                "provincia": provincia,
                "distrito": distrito,
                "municipio": posto
            })
        MAPA_LOCALIZACOES.append({
            "provincia": provincia,
            "distrito": distrito,
            "municipio": distrito
        })


def load_municipios_distritos(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def detect_municipality(texto, localidades):
    texto_norm = normalize(texto)
    for mun in localidades:
        if normalize(mun) in texto_norm:
            return mun
    return None


def load_keywords(path, idioma="portuguese"):
    path_absoluto = os.path.join(PROJECT_ROOT, path)
    with open(path_absoluto, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data.get("weather_terms"), dict):
        return data["weather_terms"].get(idioma, [])
    return data.get(idioma, [])



def load_localidades(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    localidades = []
    for provincia, distritos in data.items():
        for distrito, postos in distritos.items():
            localidades.extend(postos)
            localidades.append(distrito)
    return localidades

def carregar_dicofreg(filepath="config/freguesias_com_codigos.json"):
    filepath = os.path.join(PROJECT_ROOT, filepath)  # <-- ADICIONA ISTO
    with open(filepath, "r", encoding="utf-8") as f:
        raw = json.load(f)

    mapping = {}
    for distrito, municipios in raw.items():
        for municipio, freguesias in municipios.items():
            for freg in freguesias:
                nome = normalize(freg["nome"])
                mapping[nome] = freg["codigo"]
    return mapping


def carregar_municipios_distritos(caminho_json):
    with open(caminho_json, "r", encoding="utf-8") as f:
        dados = json.load(f)

    localidades = {}
    for distrito, municipios in dados.items():
        for municipio in municipios:
            localidades[municipio.lower()] = distrito
    return localidades


def carregar_paroquias_com_municipios(path):
    path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), path)
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    localidades = {}
    municipios = set()
    distritos = list(data.keys())

    for distrito, lista_municipios in data.items():
        for municipio in lista_municipios:
            municipios.add(municipio)
            localidades[municipio.lower()] = {
                "district": distrito,
                "municipality": municipio
            }

    return localidades, list(municipios), distritos



# --------------------------- Verificação ---------------------------
def is_in_portugal(title: str, article_text: str) -> bool:
    return False  # Moçambique only, desativa validação de Portugal


# --------------------------- Exportar eventos com vítimas ---------------------------
def guardar_disaster_db_ready(artigos: list[dict], ficheiro: str):
    """
    Export events with victims to a CSV file.
    Uses year/month/day folder organization for structured output.
    """
    colunas = [
        "ID", "title", "date", "type", "subtype", "district", "municipali",
        "parish", "dicofreg", "hour", "source", "link_extraido", "fatalities",
        "injured", "evacuated", "displaced", "missing", "sourcetype"
    ]
    artigos_filtrados = [
        a for a in artigos
        if any(a.get(chave, 0) > 0 for chave in ["fatalities", "injured", "evacuated", "displaced", "missing"])
    ]
    if not artigos_filtrados:
        print("⚠️ Nenhum artigo com vítimas encontrado.")
        return
    
    # Organize path by year/month/day if it's in the structured directory
    organized_path = organize_path_by_date(ficheiro)
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(organized_path), exist_ok=True)
    
    with open(organized_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        for artigo in artigos_filtrados:
            writer.writerow({col: artigo.get(col, "") for col in colunas})
    print(f"✅ Guardado com sucesso em {organized_path}")
    
    # For backward compatibility, also save to the original path if different
    if organized_path != ficheiro:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(ficheiro), exist_ok=True)
        
        with open(ficheiro, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=colunas)
            writer.writeheader()
            for artigo in artigos_filtrados:
                writer.writerow({col: artigo.get(col, "") for col in colunas})
        print(f"✅ [Compatibilidade] Guardado também em {ficheiro}")

# --------------------------- Guardar CSV genérico ---------------------------
def guardar_csv(caminho, artigos: list[dict]):
    """
    Save articles to a CSV file with the correct structure.
    Uses year/month/day folder organization for structured output.
    """
    if not artigos:
        print("⚠️ Nenhum artigo para guardar.")
        return

    # Define the correct column structure for the final CSV
    colunas = [
        "ID", "type", "subtype", "date", "year", "month", "day", "hour", "georef",
        "district", "municipali", "parish", "DICOFREG", "source", "sourcedate",
        "sourcetype", "page", "fatalities", "injured", "evacuated", "displaced", "missing"
    ]
    
    # Organize path by year/month/day if it's in the structured directory
    organized_path = organize_path_by_date(caminho)
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(organized_path), exist_ok=True)

    with open(organized_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        for artigo in artigos:
            writer.writerow({col: artigo.get(col, "") for col in colunas})
    print(f"✅ Guardado com sucesso em {organized_path}")
    
    # For backward compatibility, also save to the original path if different
    if organized_path != caminho:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(caminho), exist_ok=True)
        
        with open(caminho, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=colunas)
            writer.writeheader()
            for artigo in artigos:
                writer.writerow({col: artigo.get(col, "") for col in colunas})
        print(f"✅ [Compatibilidade] Guardado também em {caminho}")

# --------------------------- Guardar incremental ---------------------------
def normalize_url(url):
    parsed = urlparse(url)
    parsed = parsed._replace(query="", fragment="")
    return urlunparse(parsed)

def gerar_id(url):
    url_normalizado = normalize_url(url)
    # Make sure url_normalizado is a string before encoding
    if not isinstance(url_normalizado, str):
        url_normalizado = str(url_normalizado)
    return hashlib.md5(url_normalizado.encode("utf-8")).hexdigest()

def organize_path_by_date(caminho):
    """
    Organiza o caminho do ficheiro por estrutura de data (ano/mês/dia)
    Se o caminho estiver numa estrutura de dados, reorganiza-o hierarquicamente
    """
    if not caminho:
        return caminho
    
    try:
        # Detecta se já está numa estrutura organizada
        if "/data/" in caminho and ("/raw/" in caminho or "/structured/" in caminho or "/processed/" in caminho):
            return caminho
        
        # Extrai informação de data do nome do ficheiro se possível
        import re
        from datetime import datetime
        
        basename = os.path.basename(caminho)
        dirname = os.path.dirname(caminho)
        
        # Procura padrões de data no nome do ficheiro
        date_patterns = [
            r'(\d{4})(\d{2})(\d{2})',  # YYYYMMDD
            r'(\d{4})-(\d{2})-(\d{2})', # YYYY-MM-DD
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, basename)
            if match:
                year, month, day = match.groups()
                
                # Constrói novo caminho organizado
                if "/data/" in dirname:
                    base_data_dir = dirname.split("/data/")[0] + "/data"
                    if "/raw/" in dirname:
                        data_type = "raw"
                    elif "/structured/" in dirname:
                        data_type = "structured"
                    elif "/processed/" in dirname:
                        data_type = "processed"
                    else:
                        data_type = "structured"  # padrão
                    
                    organized_path = os.path.join(base_data_dir, data_type, year, month, day, basename)
                    return organized_path
        
        # Se não conseguir organizar por data, retorna o caminho original
        return caminho
        
    except Exception:
        # Em caso de erro, retorna o caminho original
        return caminho

def guardar_csv_incremental(caminho, artigos: list[dict]):
    """
    Guarda artigos num ficheiro CSV de forma incremental
    Cria as diretorias necessárias e organiza por estrutura de data
    """
    if not artigos:
        print("⚠️ Lista de artigos vazia, nada para guardar")
        return
    
    # Organiza o caminho por data se apropriado
    caminho_organizado = organize_path_by_date(caminho)
    
    # Cria diretoria se não existir
    os.makedirs(os.path.dirname(caminho_organizado), exist_ok=True)
    
    # Define estrutura de colunas padrão
    colunas_padrao = [
        "ID", "type", "subtype", "date", "year", "month", "day", "hour", "georef",
        "district", "municipali", "parish", "DICOFREG", "source", "sourcedate",
        "sourcetype", "page", "fatalities", "injured", "evacuated", "displaced", "missing"
    ]
    
    # Determina colunas existentes nos artigos
    if artigos:
        colunas_existentes = set()
        for artigo in artigos:
            colunas_existentes.update(artigo.keys())
        
        # Usa colunas padrão que existem nos dados, mais quaisquer colunas extras
        colunas_finais = [col for col in colunas_padrao if col in colunas_existentes]
        colunas_extras = sorted([col for col in colunas_existentes if col not in colunas_padrao])
        colunas_finais.extend(colunas_extras)
    else:
        colunas_finais = colunas_padrao
    
    try:
        with open(caminho_organizado, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=colunas_finais)
            writer.writeheader()
            for artigo in artigos:
                # Garante que todas as colunas têm valores (usa string vazia como padrão)
                linha_completa = {col: artigo.get(col, "") for col in colunas_finais}
                writer.writerow(linha_completa)
        
        print(f"✅ {len(artigos)} artigos guardados em {caminho_organizado}")
        
    except Exception as e:
        print(f"❌ Erro ao guardar CSV: {e}")
        raise

# --------------------------- Process URLs in parallel ---------------------------
def process_urls_in_parallel(urls, process_function, max_workers=5):
    """
    Process a list of URLs in parallel using a given processing function.

    Args:
        urls (list): List of URLs to process.
        process_function (callable): Function to process each URL.
        max_workers (int): Maximum number of threads to use.

    Returns:
        list: Results of processing each URL.
    """
    # Validate URLs before processing
    valid_urls = [url for url in urls if url.startswith("http")]
    if not valid_urls:
        print("⚠️ Nenhum URL válido encontrado para processar.")
        return []

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_function, url): url for url in valid_urls}
        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Error processing URL {futures[future]}: {e}")
    return results


def execute_in_parallel(items, process_function, max_threads=5):
    """
    Executes a function in parallel for a list of items.

    Args:
        items (list): List of items to process.
        process_function (callable): Function to process each item.
        max_threads (int): Maximum number of threads to use.

    Returns:
        list: Results of processing each item.
    """
    results = []
    with ThreadPoolExecutor(max_threads) as executor:
        futures = {executor.submit(process_function, item): item for item in items}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"❌ Error processing item {futures[future]}: {e}")
    return results