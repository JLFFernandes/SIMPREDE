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
from extracao.normalizador import (
    normalize,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse
import hashlib

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
    with open(ficheiro, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        for artigo in artigos_filtrados:
            writer.writerow({col: artigo.get(col, "") for col in colunas})

# --------------------------- Guardar CSV genérico ---------------------------
def guardar_csv(caminho, artigos: list[dict]):
    """
    Save articles to a CSV file with the correct structure.
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

    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        for artigo in artigos:
            writer.writerow({col: artigo.get(col, "") for col in colunas})
    print(f"✅ Guardado com sucesso em {caminho}")

# --------------------------- Guardar incremental ---------------------------
def normalize_url(url):
    parsed = urlparse(url)
    parsed = parsed._replace(query="", fragment="")
    return urlunparse(parsed)

def gerar_id(url):
    url_normalizado = normalize_url(url)
    return hashlib.md5(url_normalizado.encode("utf-8")).hexdigest()

def guardar_csv_incremental(caminho, novos_artigos: list[dict]):
    if not novos_artigos:
        print("⚠️ Nenhum novo artigo recebido.")
        return

    print(f"📥 Novos artigos recebidos: {len(novos_artigos)}")

    # Ensure all articles have valid IDs
    for artigo in novos_artigos:
        if not artigo.get("ID"):
            artigo["ID"] = hashlib.md5((artigo.get("title", "") + artigo.get("link", "")).encode()).hexdigest()

    novos_artigos = [a for a in novos_artigos if a.get("ID")]
    if not novos_artigos:
        print("⚠️ Nenhum artigo com ID válido encontrado.")
        return

    # Check for existing articles in the file
    if os.path.exists(caminho):
        with open(caminho, "r", encoding="utf-8") as f:
            existentes = {row["ID"] for row in csv.DictReader(f)}
    else:
        existentes = set()

    print(f"📂 IDs existentes no arquivo: {len(existentes)}")

    # Filter out articles with duplicate IDs
    artigos_unicos = [a for a in novos_artigos if a["ID"] not in existentes]
    print(f"🆕 Artigos únicos para salvar: {len(artigos_unicos)}")

    if not artigos_unicos:
        print("⚠️ Nenhum artigo único para salvar.")
        return

    # Append articles to the single CSV file in batches of 10
    with open(caminho, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=artigos_unicos[0].keys())
        if os.stat(caminho).st_size == 0:  # Write header if file is empty
            writer.writeheader()

        batch_size = 10
        for i in range(0, len(artigos_unicos), batch_size):
            batch = artigos_unicos[i:i + batch_size]
            for artigo in batch:
                writer.writerow(artigo)
            print(f"💾 Guardados {len(batch)} artigos no arquivo {caminho} (batch {i // batch_size + 1})")


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