import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# --------------------------
# 1. EXTRAÇÃO DOS ALERTAS IPMA
# --------------------------

distritos = [
    "Aveiro", "Beja", "Braga", "Bragança", "Castelo Branco", "Coimbra", "Évora", "Faro",
    "Guarda", "Leiria", "Lisboa", "Portalegre", "Porto", "Santarém", "Setúbal", "Viana do Castelo",
    "Vila Real", "Viseu"
]

urls_ipma = {
    "Trovoada": "https://www.ipma.pt/pt/otempo/prev-sam/?p=thunderstorm",
    "Agitação Marítima": "https://www.ipma.pt/pt/otempo/prev-sam/?p=coastalevent",
    "Chuva": "https://www.ipma.pt/pt/otempo/prev-sam/?p=rain",
    "Vento": "https://www.ipma.pt/pt/otempo/prev-sam/?p=wind",
    "Neve/Gelo": "https://www.ipma.pt/pt/otempo/prev-sam/?p=snow-ice",
    "Temperatura Baixa": "https://www.ipma.pt/pt/otempo/prev-sam/?p=low-temperature",
    "Temperatura Alta": "https://www.ipma.pt/pt/otempo/prev-sam/?p=high-temperature",
    "Nevoeiro": "https://www.ipma.pt/pt/otempo/prev-sam/?p=fog"
}

dados_alertas = {distrito: {tipo: "Sem dados" for tipo in urls_ipma.keys()} for distrito in distritos}

for tipo_aviso, url in urls_ipma.items():
    print(f"🔍 Recolhendo dados para: {tipo_aviso}...")

    response = requests.get(url)
    time.sleep(2)
    if response.status_code != 200:
        print(f"❌ Erro ao acessar {url}")
        continue

    soup = BeautifulSoup(response.text, "html.parser")
    avisos = soup.select("div.ww-t0, div.ww-t1")

    for aviso in avisos:
        distrito_elem = aviso.find("div", class_="ww-reg")
        cor_alerta_elem = aviso.select_one(".sam-day div, .sam-day-l div")
        if not distrito_elem or not cor_alerta_elem:
            continue

        distrito = distrito_elem.text.strip()
        cor_alerta_classes = cor_alerta_elem.get("class", [])

        if "wc-yellow" in cor_alerta_classes:
            nivel_alerta = "Amarelo"
        elif "wc-orange" in cor_alerta_classes:
            nivel_alerta = "Laranja"
        elif "wc-red" in cor_alerta_classes:
            nivel_alerta = "Vermelho"
        elif "wc-green" in cor_alerta_classes:
            nivel_alerta = "Verde"
        elif "wc-gray" in cor_alerta_classes:
            nivel_alerta = "Cinzento (Informação em atualização)"
        else:
            nivel_alerta = "Sem dados"

        if distrito in distritos:
            dados_alertas[distrito][tipo_aviso] = nivel_alerta

# --------------------------
# 2. MERGE DOS ALERTAS DIRETAMENTE COM A TABELA `ocorrencias` NO BANCO DE DADOS SQLITE
# --------------------------

conn = sqlite3.connect("dados_ocorrencias.db")
cursor = conn.cursor()

# Verificar quais distritos têm ocorrências ativas
cursor.execute("SELECT DISTINCT distrito FROM ocorrencias WHERE estado = 'Ativa'")
distritos_ativos = {row[0].strip() for row in cursor.fetchall()}

# Filtrar apenas os alertas dos distritos que têm ocorrências ativas
dados_alertas_filtrados = {d: v for d, v in dados_alertas.items() if d in distritos_ativos}

if not dados_alertas_filtrados:
    print("⚠️ Nenhum distrito do IPMA corresponde a uma ocorrência ativa. Nenhum dado será atualizado.")
    conn.close()
    exit()

# Adicionar colunas de alertas meteorológicos na tabela `ocorrencias`, caso ainda não existam
colunas_alertas = ["trovoada", "agitacao_maritima", "chuva", "vento", "neve_gelo", "temperatura_baixa", "temperatura_alta", "nevoeiro"]
for coluna in colunas_alertas:
    try:
        cursor.execute(f"ALTER TABLE ocorrencias ADD COLUMN {coluna} TEXT DEFAULT 'Sem dados'")
    except sqlite3.OperationalError:
        pass  # A coluna já existe, ignorar erro

# Atualizar os alertas diretamente na tabela `ocorrencias`
for distrito, alertas in dados_alertas_filtrados.items():
    cursor.execute('''
        UPDATE ocorrencias
        SET trovoada = ?, agitacao_maritima = ?, chuva = ?, vento = ?, neve_gelo = ?, 
            temperatura_baixa = ?, temperatura_alta = ?, nevoeiro = ?
        WHERE distrito = ? AND estado = 'Ativa'
    ''', list(alertas.values()) + [distrito])

conn.commit()
conn.close()

print("✅ Dados dos alertas meteorológicos foram mesclados na tabela `ocorrencias` no banco de dados!")

# --------------------------
# 3. MERGE DOS ALERTAS COM O FICHEIRO EXCEL
# --------------------------

try:
    df_ocorrencias = pd.read_excel("dados_ocorrencias.xlsx")
except Exception as e:
    print("Erro ao carregar 'dados_ocorrencias.xlsx':", e)
    df_ocorrencias = pd.DataFrame()

df_alertas = pd.DataFrame.from_dict(dados_alertas_filtrados, orient='index').reset_index()
df_alertas.columns = ["distrito", "Trovoada", "Agitação Marítima", "Chuva", "Vento", "Neve/Gelo", "Temperatura Baixa", "Temperatura Alta", "Nevoeiro"]

if not df_ocorrencias.empty:
    df_ocorrencias.rename(columns=lambda x: x.strip().lower(), inplace=True)
    df_alertas.rename(columns=lambda x: x.strip().lower(), inplace=True)

    df_ocorrencias_ativas = df_ocorrencias[df_ocorrencias["estado"] == "Ativa"]

    if "distrito" in df_ocorrencias_ativas.columns and "distrito" in df_alertas.columns:
        df_merge = pd.merge(df_ocorrencias_ativas, df_alertas, on="distrito", how="left")  # Mantém todas as ocorrências ativas
        df_merge.to_excel("dados_ocorrencias.xlsx", index=False)
        print("✅ Dados do IPMA mesclados com ocorrências ativas no ficheiro 'dados_ocorrencias.xlsx'!")
    else:
        print("⚠️ Erro: A coluna 'distrito' ou 'estado' não foi encontrada corretamente. Nenhum merge realizado.")
else:
    print("⚠️ Nenhuma ocorrência ativa encontrada no Excel. Nenhuma alteração feita.")
