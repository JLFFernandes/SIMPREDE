import sqlite3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

# Configuração do Selenium
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# Inicializar o WebDriver
service = Service("/usr/local/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=options)

# URL do site
url = "https://prociv-portal.geomai.mai.gov.pt/arcgis/apps/dashboards/47b772e5c8b447399eb5dfc1cc0dcf00"
driver.get(url)

# Aguardar carregamento da página
time.sleep(5)

# Capturar o HTML da página
html_source = driver.page_source
driver.quit()

# Processar HTML com BeautifulSoup
soup = BeautifulSoup(html_source, "html.parser")

# Encontrar todas as divs com class="external-html"
divs_external = soup.find_all("div", class_="external-html")

# Lista para armazenar os dados extraídos
dados_ocorrencias = []

for div in divs_external:
    # Captura o tipo de ocorrência e ignora se não existir
    tipo_ocorrencia_elem = div.find("th")
    if not tipo_ocorrencia_elem:
        continue  # Ignora esta entrada e passa para a próxima
    
    tipo_ocorrencia = tipo_ocorrencia_elem.text.strip()

    # Capturar Data e Hora separadamente e converter para formato adequado
    data_hora_elem = div.find("span", style="font-size:10px")
    if data_hora_elem:
        data_hora = data_hora_elem.text.strip()
        partes_data_hora = data_hora.split(", ")  # Exemplo: ["03/03/2025", "20:07"]

        # Converter a data para formato SQLite (YYYY-MM-DD)
        try:
            data = datetime.strptime(partes_data_hora[0], "%d/%m/%Y").strftime("%Y-%m-%d")  # Ajusta o formato
        except ValueError:
            data = "0000-00-00"  # Valor padrão caso falhe a conversão

        # Converter a hora para formato SQLite (HH:MM:SS)
        try:
            hora = datetime.strptime(partes_data_hora[1], "%H:%M").strftime("%H:%M:%S")  # Ajusta o formato
        except ValueError:
            hora = "00:00:00"  # Valor padrão caso falhe a conversão
    else:
        data, hora = "0000-00-00", "00:00:00"

    # Capturar Região e Concelho separadamente
    regiao_elem = div.find_all("span", style="font-size:11px")
    
    if len(regiao_elem) > 0:
        partes_regiao = regiao_elem[0].text.strip().split("|")
        regiao = partes_regiao[0].strip() if len(partes_regiao) > 0 else "Não encontrado"
        concelho = partes_regiao[1].strip() if len(partes_regiao) > 1 else "Não encontrado"
    else:
        regiao, concelho = "Não encontrado", "Não encontrado"

    # Capturar Localidade e Rua separadamente
    if len(regiao_elem) > 1:
        partes_localidade = regiao_elem[1].text.strip().split("|")
        localidade = partes_localidade[0].strip() if len(partes_localidade) > 0 else "Não encontrado"
        rua = partes_localidade[1].strip() if len(partes_localidade) > 1 else "Não encontrado"
    else:
        localidade, rua = "Não encontrado", "Não encontrado"

    # Inicializar os valores como 0 (número)
    operacionais = 0
    meios_terrestres = 0
    meios_aereos = 0

    # Mapeamento de ícones específicos para cada categoria
    tds_com_img = div.find_all("td", style=True)  # Procura todos os <td> que tenham "style"
    
    for td in tds_com_img:
        img = td.find("img")
        if img:
            valor = td.get_text(strip=True).replace("\u00a0", "").strip()  # Remove espaços invisíveis
            valor = int(valor) if valor.isdigit() else 0  # Converte para número ou define 0 se não for numérico

            src = img["src"]  # URL da imagem

            # Ajustar os ícones corretos com base no URL da imagem
            if "dd30d7cf42b54780a4e7536ff63b8e0d" in src:  # Operacionais
                operacionais = valor
            elif "a9b1294a85a94375accee1c28a42be9a" in src:  # Meios Terrestres
                meios_terrestres = valor
            elif "544c5a61a01543d89aaa1f434acd1916" in src:  # Meios Aéreos
                meios_aereos = valor

    # Adiciona ao dataset com os valores corretos
    dados_ocorrencias.append((tipo_ocorrencia, data, hora, regiao, concelho, localidade, rua, operacionais, meios_terrestres, meios_aereos, "Ativa"))

# Conectar ao SQLite e criar tabela
with sqlite3.connect("dados_ocorrencias.db") as conn:
    cursor = conn.cursor()

    # Criar a tabela se não existir
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ocorrencias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo_ocorrencia TEXT,
        data DATE,
        hora TIME,
        regiao TEXT,
        concelho TEXT,
        distrito TEXT,
        localidade TEXT,
        rua TEXT,
        operacionais INTEGER,
        meios_terrestres INTEGER,
        meios_aereos INTEGER,
        estado TEXT DEFAULT 'Ativa',
        UNIQUE(tipo_ocorrencia, data, hora, regiao, concelho, localidade, rua) 
    )
    """)

    # Atualizar todas as ocorrências para "Inativa" antes de processar as novas
    cursor.execute("UPDATE ocorrencias SET estado = 'Inativa'")

    # Carregar a base de dados de municípios e distritos
    df_municipios = pd.read_excel("/home/pcc/webscraping/V6/pcc_municipios.xlsx")
    df_municipios["concelho"] = df_municipios["concelho"].str.strip().str.lower()

    # Inserir ou atualizar dados na tabela
    for ocorrencia in dados_ocorrencias:
        concelho = ocorrencia[4].strip().lower()
        distrito = df_municipios.loc[df_municipios['concelho'] == concelho, 'distrito']
        distrito_value = distrito.values[0] if not distrito.empty else "Desconhecido"
        
        ocorrencia_com_distrito = ocorrencia[:5] + (distrito_value,) + ocorrencia[5:]

        cursor.execute("""
        INSERT INTO ocorrencias (tipo_ocorrencia, data, hora, regiao, concelho, distrito, localidade, rua, operacionais, meios_terrestres, meios_aereos, estado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tipo_ocorrencia, data, hora, regiao, concelho, localidade, rua)
        DO UPDATE SET 
            operacionais = excluded.operacionais,
            meios_terrestres = excluded.meios_terrestres,
            meios_aereos = excluded.meios_aereos,
            estado = 'Ativa'
        """, ocorrencia_com_distrito)

    print("✅ Dados extraídos e salvos no banco de dados SQLite ('dados_ocorrencias.db').")

    # Criar DataFrame a partir do banco de dados
    df = pd.read_sql_query("SELECT * FROM ocorrencias", conn)

    # Salvar no Excel
    df.to_excel("dados_ocorrencias.xlsx", index=False)

print("📊 Dados também foram exportados para 'dados_ocorrencias.xlsx'.")
