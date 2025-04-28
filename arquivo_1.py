import os
import time
import re
import requests
import requests.compat
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
import pandas as pd

# Palavras-chave para identificar as notícias de interesse
KEYWORDS = ["cheia", "inundacao", "inundação"]

def contem_keywords(texto, keywords):
    texto_lower = texto.lower()
    return any(kw in texto_lower for kw in keywords)

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.page_load_strategy = "eager"
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(240)  # Timeout de 240 segundos
    return driver

def carregar_pagina_com_selenium(url):
    driver = setup_driver()
    try:
        driver.get(url)
    except Exception as e:
        print(f"Erro no driver.get para {url}: {e}")
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "li.menu-pages-replay-date-hour"))
        )
    except Exception as e:
        print(f"Timeout ou erro ao esperar os elementos: {e}")
    page_source = driver.page_source
    driver.quit()
    return page_source

def extrair_links_arquivados(html, url_base):
    soup = BeautifulSoup(html, 'html.parser')
    anchors = soup.select("li.menu-pages-replay-date-hour a")
    print(f"[DEBUG] Número de anchors encontrados com o seletor 'li.menu-pages-replay-date-hour a': {len(anchors)}")
    if not anchors:
        debug_html = soup.prettify()[:1000]
        print("[DEBUG] Primeira parte do HTML retornado:")
        print(debug_html)
    links = []
    for a in anchors:
        href = a.get("href")
        if href:
            absolute_url = requests.compat.urljoin(url_base, href)
            replay_timestamp = a.get("replay-timestamp")
            replay_url = a.get("replay-url")
            texto = a.get_text(strip=True)
            links.append({
                'url': absolute_url,
                'replay_timestamp': replay_timestamp,
                'replay_url': replay_url,
                'texto': texto
            })
    return links

def get_soup(url):
    """
    Faz requisição GET e retorna o conteúdo como um objeto BeautifulSoup.
    """
    try:
        response = requests.get(url, timeout=120)
    except Exception as e:
        print(f"Erro ao acessar a URL: {url}. Exceção: {e}")
        return None
    if response.status_code == 200:
        return BeautifulSoup(response.text, 'html.parser')
    else:
        print("Erro ao carregar a URL:", url, "Código de status:", response.status_code)
        return None

def extrair_data_da_url(url):
    """
    Extrai o timestamp presente no URL de arquivamento e retorna a data no formato "AAAA-MM-DD".
    Por exemplo, de "20160211180223" retorna "2016-02-11".
    """
    m = re.search(r"wayback/(\d{14})/", url)
    if m:
        full_timestamp = m.group(1)  # ex: "20160211180223"
        yyyy = full_timestamp[0:4]
        mm = full_timestamp[4:6]
        dd = full_timestamp[6:8]
        return f"{yyyy}-{mm}-{dd}"
    return "Data não identificada"

def processar_artigo_regex(url):
    """
    Processa a página (ou o conteúdo de um frame/iframe se houver) e
    procura nas tags de título (h1-h5) as palavras-chave.
    Retorna uma lista de notícias com as informações necessárias.
    """
    soup = get_soup(url)
    if not soup:
        return []
    
    # Se existir iframe ou frame, usa o conteúdo dele
    frame_tag = soup.find('iframe') or soup.find('frame')
    if frame_tag:
        frame_src = frame_tag.get('src')
        url_frame = urljoin(url, frame_src)
        print("Frame encontrado. Acessando o conteúdo em:", url_frame)
        soup = get_soup(url_frame)
        if not soup:
            return []
    
    candidate_tags = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5'])
    
    pattern = re.compile(r'\b(cheia[s]?|inundaç[ãa]o[s]?)\b', re.IGNORECASE)
    location_pattern = re.compile(r'\bem\s+([A-Z][a-zÀ-ÿ]+(?:\s+[A-Z][a-zÀ-ÿ]+)*)')
    
    fonte = "arquivo.pt"
    data_artigo = extrair_data_da_url(url)
    # Extração de Ano, Mês e Dia a partir da data extraída
    if data_artigo != "Data não identificada":
        ano = data_artigo[0:4]
        mes = data_artigo[5:7]
        dia = data_artigo[8:10]
    else:
        ano, mes, dia = "N/A", "N/A", "N/A"
    
    news = []
    for tag in candidate_tags:
        text = tag.get_text(strip=True)
        if text and pattern.search(text):
            location_match = location_pattern.search(text)
            location = location_match.group(1) if location_match else "Local não identificado"
            news.append({
                'URL Artigo': url,
                'Título': text,
                'Local': location,
                'Data': data_artigo,
                'Ano': ano,
                'Mês': mes,
                'Dia': dia,
                'Fonte': fonte
            })
    return news

def main():
    url_inicial = "https://arquivo.pt/wayback/20150101180216/http://www.jn.pt/paginainicial/"
    print(f"Acessando a página inicial: {url_inicial}")
    
    # Solicita ao usuário o intervalo de pesquisa:
    start_year = input("Digite o ano de início (ex: 2015): ").strip()
    start_month = input("Digite o mês de início (ex: 01 para Janeiro): ").strip()
    end_year = input("Digite o ano de fim (ex: 2016): ").strip()
    end_month = input("Digite o mês de fim (ex: 12 para Dezembro): ").strip()
    
    # Validação simples dos inputs:
    if not (start_year.isdigit() and len(start_year)==4 and end_year.isdigit() and len(end_year)==4):
        print("Anos inválidos. Por favor, digite anos com 4 dígitos.")
        return
    if not (start_month.isdigit() and len(start_month)==2 and end_month.isdigit() and len(end_month)==2):
        print("Meses inválidos. Por favor, digite os meses com 2 dígitos (ex: 01, 02, ..., 12).")
        return

    start_period = int(start_year + start_month)
    end_period   = int(end_year + end_month)
    if start_period > end_period:
        print("O intervalo de datas está inválido: a data de início deve ser menor ou igual à data de fim.")
        return

    html = carregar_pagina_com_selenium(url_inicial)
    links_extraidos = extrair_links_arquivados(html, url_inicial)
    print(f"Foram encontrados {len(links_extraidos)} links de artigos na página.")
    
    links_filtrados = []
    for link in links_extraidos:
        timestamp = link.get("replay_timestamp")
        if timestamp and len(timestamp) >= 6:
            ts_val = int(timestamp[:6])
            if start_period <= ts_val <= end_period:
                links_filtrados.append(link)
        else:
            # Se não houver timestamp válido, o link é ignorado para a pesquisa em intervalo
            continue
    
    print(f"Após filtrar, foram encontrados {len(links_filtrados)} links para o intervalo de {start_year}-{start_month} até {end_year}-{end_month}.\n")
    
    todas_noticias = []
    for item in links_filtrados:
        link = item['url']
        print(f"Processando artigo: {link}")
        noticias = processar_artigo_regex(link)
        if noticias:
            print("\nNotícias encontradas:")
            for noticia in noticias:
                print(f" - Título: {noticia['Título']}")
                print(f"   Local: {noticia['Local']}")
                print(f"   Data: {noticia['Data']}")
                print(f"   Ano: {noticia['Ano']}, Mês: {noticia['Mês']}, Dia: {noticia['Dia']}")
                print(f"   Fonte: {noticia['Fonte']}")
                print("=" * 40)
                todas_noticias.append(noticia)
        else:
            print("Nenhuma notícia com as palavras-chave foi encontrada neste artigo.")
        print()
        time.sleep(1)
    
    if todas_noticias:
        df_novos = pd.DataFrame(todas_noticias)
        nome_arquivo = "noticias_encontradas.xlsx"
        
        if os.path.exists(nome_arquivo):
            try:
                df_existente = pd.read_excel(nome_arquivo)
                df_combinado = pd.concat([df_existente, df_novos], ignore_index=True)
                # Remove duplicados considerando os campos que definem a unicidade da notícia
                df_combinado.drop_duplicates(
                    subset=["Título", "Local", "Data", "Ano", "Mês", "Dia", "Fonte"],
                    keep="first",
                    inplace=True
                )
            except Exception as e:
                print("Erro ao carregar o ficheiro Excel existente. Os novos dados serão salvos sem junção.", e)
                df_combinado = df_novos
        else:
            df_combinado = df_novos
        
        # Reordena as colunas conforme solicitado
        colunas_ordem = ["URL Artigo", "Título", "Local", "Data", "Ano", "Mês", "Dia", "Fonte"]
        df_combinado = df_combinado[colunas_ordem]
        
        df_combinado.to_excel(nome_arquivo, index=False)
        print(f"\nFicheiro Excel '{nome_arquivo}' atualizado com sucesso!")
    else:
        print("Nenhuma notícia encontrada para salvar no ficheiro Excel.")

if __name__ == "__main__":
    main()
