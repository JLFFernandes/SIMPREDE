import streamlit as st
import pandas as pd
import io
import altair as alt
import pydeck as pdk
from supabase import create_client, Client
from sklearn.linear_model import LinearRegression
import numpy as np
import plotly.express as px
from sklearn.ensemble import RandomForestRegressor
import base64
from pathlib import Path

# --- Configuração da página (DEVE SER A PRIMEIRA COMANDO STREAMLIT) ---
st.set_page_config(layout="wide", page_title="SIMPREDE", page_icon="🌍")

def get_base64_image(image_path):
    try:
        # Obter o diretório onde este script está localizado
        script_dir = Path(__file__).parent
        # Criar caminho completo para a imagem
        full_image_path = script_dir / image_path
        
        with open(full_image_path, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode("utf-8")
    except FileNotFoundError:
        # Retornar um placeholder ou string vazia se a imagem não for encontrada
        st.warning(f"Image file {image_path} not found at {full_image_path}. Using placeholder.")
        # Criar um PNG transparente simples 1x1 como alternativa
        import io
        from PIL import Image
        img = Image.new('RGBA', (1, 1), (0, 0, 0, 0))
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        return base64.b64encode(buffer.getvalue()).decode("utf-8")

try:
    logo_uab = get_base64_image("UAB.png")
    logo_lei = get_base64_image("LEI.png")
except Exception as e:
    st.error(f"Error loading images: {e}")
    # Usar placeholders vazios
    logo_uab = ""
    logo_lei = ""

url = "https://kyrfsylobmsdjlrrpful.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt5cmZzeWxvYm1zZGpscnJwZnVsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDUzNTY4MzEsImV4cCI6MjA2MDkzMjgzMX0.DkPGAw89OH6MPNnCvimfsVJICr5J9n9hcgdgF17cP34"
supabase: Client = create_client(url, key)


# Definir cores globais consistentes
COR_FLOOD = "#1f77b4"
COR_LANDSLIDE = "#ff7f0e"

# --- Cores consistentes globais ---
COR_HEX = {
    "Flood": "#1f77b4",
    "Landslide": "#ff7f0e"
}
COR_RGBA = {
    "Flood": [31, 119, 180, 160],
    "Landslide": [255, 127, 14, 160]
}


logo_base64 = get_base64_image("UAB.png")

st.markdown(f"""
    <style>
        .title-box {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            background-color: #f0f2f6;
            border-radius: 16px;
            padding: 1.5em 2em;
            margin-bottom: 1.5em;
            box-shadow: 0px 4px 12px rgba(0, 0, 0, 0.05);
        }}

        .title-text {{
            text-align: center;
            flex: 1;
        }}

        .title-text h1 {{
            font-size: 2.8em;
            margin-bottom: 0.2em;
            color: #003366;
        }}

        .title-text h3 {{
            margin: 0;
            font-weight: normal;
            color: #333;
        }}

        .subtitle-small {{
            margin-top: 0.5em;
            font-size: 1.1em;
            color: #000000;
        }}

        .logo-img {{
            display: flex;
            align-items: center;
        }}

        .logo-img img {{
            height: 170px;
        }}

        .logo-left {{
            margin-right: 2em;
        }}

        .logo-right {{
            margin-left: -1em;
        }}

        hr {{
            border: none;
            border-top: 1px solid #ccc;
            margin: 1em 0 2em 0;
        }}

        /* Hide mapbox attribution */
        .mapboxgl-ctrl-attrib {{
            display: none !important;
        }}
        
        .mapboxgl-ctrl-bottom-right {{
            display: none !important;
        }}

        .mapboxgl-ctrl-bottom-left {{
            display: none !important;
        }}

        /* Hide plotly mapbox attribution specifically */
        .js-plotly-plot .plotly .mapboxgl-ctrl-attrib {{
            display: none !important;
        }}

        .js-plotly-plot .plotly .mapboxgl-ctrl-bottom-right {{
            display: none !important;
        }}

        .js-plotly-plot .plotly .mapboxgl-ctrl-bottom-left {{
            display: none !important;
        }}

        /* Additional CSS to hide attribution */
        .mapboxgl-ctrl {{
            display: none !important;
        }}

        .attribution {{
            display: none !important;
        }}

        /* Hide any text containing attribution */
        *[class*="attrib"] {{
            display: none !important;
        }}

        /* Hide bottom controls */
        .mapboxgl-ctrl-bottom-right,
        .mapboxgl-ctrl-bottom-left,
        .mapboxgl-control-container {{
            display: none !important;
        }}
    </style>

    <div class="title-box">
        <div class="logo-img logo-left">
            {f'<img src="data:image/png;base64,{logo_lei}" alt="Logotipo LEI">' if logo_lei else '<div style="width: 170px; height: 170px;"></div>'}
        </div>
        <div class="title-text">
            <h1>SIMPREDE</h1>
            <h3>Sistema Inteligente de Monitorização e Previsão de Desastres Naturais</h3>
            <div class="subtitle-small">Universidade Aberta</div>
        </div>
        <div class="logo-img logo-right">
            {f'<img src="data:image/png;base64,{logo_uab}" alt="Logotipo UAb">' if logo_uab else '<div style="width: 170px; height: 170px;"></div>'}
        </div>
    </div>
    <hr>
""", unsafe_allow_html=True)

# --- Carregamento de dados ---
@st.cache_data
def carregar_disasters():
    todos_os_dados = []
    passo = 1000
    inicio = 0

    while True:
        response = supabase.table("disasters").select(
            "id, year, month, type, subtype, date"
        ).range(inicio, inicio + passo - 1).execute()

        dados_pagina = response.data

        if not dados_pagina:
            break  # terminou a leitura

        todos_os_dados.extend(dados_pagina)
        inicio += passo

    df = pd.DataFrame(todos_os_dados)

    df["type"] = df["type"].str.capitalize()
    df = df[df["type"].isin(["Flood", "Landslide"])]
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df.dropna(subset=["year", "month", "date"])




@st.cache_data
def carregar_localizacoes_disasters():
    todos = []
    passo = 1000
    inicio = 0

    while True:
        response = supabase.table("location").select(
            "id, latitude, longitude, district, municipality"
        ).range(inicio, inicio + passo - 1).execute()

        dados = response.data
        if not dados:
            break

        todos.extend(dados)
        inicio += passo

    df = pd.DataFrame(todos)
    return df.dropna(subset=["latitude", "longitude"])


@st.cache_data
def carregar_scraper():
    todos = []
    passo = 1000
    inicio = 0

    while True:
        response = supabase.table("google_scraper_ocorrencias").select(
            "id, type, year, month, latitude, longitude, district"
        ).range(inicio, inicio + passo - 1).execute()

        dados = response.data
        if not dados:
            break

        todos.extend(dados)
        inicio += passo

    df = pd.DataFrame(todos)
    df["type"] = df["type"].str.capitalize()
    df = df[df["type"].isin(["Flood", "Landslide"])]
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    return df.dropna(subset=["year", "month", "latitude", "longitude"])


#df = carregar_disasters()
#st.write("✅ Registos carregados após correção:", len(df))


# Dicionário com correções conhecidas de nomes de distritos
substituir_distritos = {
    "Azores ": "Azores",
    "Açores": "Azores",
    "Azores - Terceira": "Azores",
    "Lisboa ": "Lisboa",
    "Lisboaa": "Lisboa",
    "Porto ": "Porto",
    "Santarem": "Santarém",
    "Santarém ": "Santarém",
    "Braganca": "Bragança",
    "Evora": "Évora",
    "Bejá": "Beja",
    "Trofa": "Porto",
    "Viana Do Castelo": "Viana do Castelo",
    "Viana do castelo": "Viana do Castelo",
    "Setubal": "Setúbal",
    "Coímbra": "Coimbra",
    "Faro ": "Faro",
    "Portalegre ": "Portalegre",
    # Podes adicionar mais aqui conforme identificares
}

df_loc_disasters = carregar_localizacoes_disasters()

# Normalizar capitalização e espaços
df_loc_disasters["district"] = df_loc_disasters["district"].str.strip().str.title()

# Aplicar substituições
df_loc_disasters["district"] = df_loc_disasters["district"].replace(substituir_distritos)



# Ver os distritos já limpos
distritos_corrigidos = sorted(df_loc_disasters["district"].dropna().unique())
#st.write("Distritos após padronização:", distritos_corrigidos)




@st.cache_data
def carregar_human_impacts():
    todos = []
    passo = 1000
    inicio = 0

    while True:
        response = supabase.table("human_impacts").select(
            "id, fatalities"
        ).range(inicio, inicio + passo - 1).execute()

        dados = response.data
        if not dados:
            break

        todos.extend(dados)
        inicio += passo

    return pd.DataFrame(todos)


# --- Carregamento ---
df_disasters_raw = carregar_disasters()
df_scraper = carregar_scraper()
df_disasters = df_disasters_raw.groupby(["year", "month", "type"]).size().reset_index(name="ocorrencias")
df_human_impacts = carregar_human_impacts()



# === Parte 1 ===
st.markdown("<h2 style='text-align: center;'>Ocorrências Históricas de Desastres (1865 - 2025)<br><span style='font-size: 0.8em; color: #777;'>(Historical Disaster Occurrences 1865 - 2025)</span></h2>", unsafe_allow_html=True)


# --- Agrupar dados históricos por mês/tipo ---
df_disasters_ano_tipo = df_disasters_raw.groupby(["year", "type"]).size().reset_index(name="ocorrencias")
df_disasters["date"] = pd.to_datetime(
    df_disasters["year"].astype(str) + "-" + df_disasters["month"].astype(str).str.zfill(2) + "-01"
)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
    "<h4 style='text-align: center;'>Dados históricos agregados por ano e tipologia<br><span style='font-size: 0.7em; color: #777;'>(Historical data aggregated by year and type)</span></h4>",
    unsafe_allow_html=True
)

    df_ano_tipo = df_disasters_raw.groupby(["year", "type"]).size().reset_index(name="ocorrencias")
    st.dataframe(df_ano_tipo, use_container_width=True)

with col2:
    st.markdown(
    "<h4 style='text-align: center;'>Vítimas mortais por distrito e tipologia de desastre<br><span style='font-size: 0.7em; color: #777;'>(Fatalities by district and disaster type)</span></h4>",
    unsafe_allow_html=True
)


    # Merge com impactos humanos e localização
    df_merged = pd.merge(
        df_disasters_raw,
        df_human_impacts[["id", "fatalities"]],
        on="id",
        how="left"
    )

    df_merged = pd.merge(
        df_merged,
        df_loc_disasters[["id", "district"]],
        on="id",
        how="left"
    )

    # Limpeza
    df_merged["fatalities"] = pd.to_numeric(df_merged["fatalities"], errors="coerce").fillna(0)
    df_merged["district"] = df_merged["district"].astype(str).str.strip().str.title()
    df_merged["type"] = df_merged["type"].str.capitalize()

    # Remover distritos nulos ou vazios
    df_merged = df_merged[df_merged["district"].notna() & (df_merged["district"] != "")]

    # Agrupar por distrito e tipo
    df_grouped = df_merged.groupby(["district", "type"])["fatalities"].sum().reset_index()
    df_grouped = df_grouped[df_grouped["fatalities"] > 0]

    # Escala do eixo Y com margem de 10%
    max_fatal = df_grouped["fatalities"].max()
    y_lim = max_fatal * 1.1

    chart = alt.Chart(df_grouped).mark_bar().encode(
        x=alt.X("district:N", title=None, sort="-y"),
        y=alt.Y("fatalities:Q", title="Nº de Vítimas Mortais", scale=alt.Scale(domain=[0, y_lim])),
        color=alt.Color(
            "type:N",
            title="Tipo",
            scale=alt.Scale(
                domain=["Flood", "Landslide"],
                range=[COR_HEX["Flood"], COR_HEX["Landslide"]]
            ),
            legend=alt.Legend(title="type")
        ),
        tooltip=["district", "type", "fatalities"]
    ).properties(
        height=400,
        width=600
    )

    if df_grouped.empty:
        st.warning("Sem dados de vítimas mortais disponíveis.")
    else:
        st.altair_chart(chart, use_container_width=True)



# Cores globais
COR_FLOOD = [31, 119, 180, 160]       # Azul
COR_LANDSLIDE = [255, 127, 14, 160]   # Laranja

with col3:
    st.markdown(
    "<h4 style='text-align: center;'>Ocorrências Históricas no Mapa<br><span style='font-size: 0.7em; color: #777;'>(Historical Occurrences on Map)</span></h4>",
    unsafe_allow_html=True
)

    df_merge1 = pd.merge(df_disasters_raw, df_loc_disasters, on="id")

    tipo_mapa1_val = st.session_state.get("mapa1", "Todos")

    if tipo_mapa1_val != "Todos":
        df_merge1 = df_merge1[df_merge1["type"] == tipo_mapa1_val]
        cor = COR_FLOOD if tipo_mapa1_val == "Flood" else COR_LANDSLIDE
        df_merge1["color"] = [cor] * len(df_merge1)
    else:
        df_merge1["color"] = df_merge1["type"].map({
            "Flood": COR_FLOOD,
            "Landslide": COR_LANDSLIDE
        })

    if not df_merge1.empty:
        fig_map1 = px.scatter_map(
            df_merge1,
            lat="latitude",
            lon="longitude",
            color="type",
            hover_data=["district", "municipality", "year", "month"],
            zoom=4.5, 
            height=400,
            center={"lat": 39.5, "lon": -8.0},  # Center on Portugal mainland
            color_discrete_map={"Flood": COR_HEX["Flood"], "Landslide": COR_HEX["Landslide"]}
        )
        fig_map1.update_layout(
            mapbox_style="stamen-terrain",
            showlegend=True,
            margin={"r":0,"t":30,"l":0,"b":0}
        )
        st.plotly_chart(fig_map1, use_container_width=True)

        # Filtro abaixo do mapa
        tipo_mapa_1 = st.radio(
            "Selecionar tipo de desastre (mapa):",
            ["Todos", "Flood", "Landslide"],
            horizontal=True,
            key="mapa1"
        )
    else:
        st.warning("Sem dados de localização disponíveis.")


st.markdown("""
<div style='font-size: 0.9em; color: #555; margin-top: 1em; text-align: center;'>
<strong>Fontes:</strong><br>
<a href="https://idlcc.fc.ul.pt/pdf/Zezere_2014_DISASTER.pdf" target="_blank">Disaster (Zêzere et al., 2014)</a> |
<a href="https://eswd.eu/" target="_blank">ESWD</a> |
<a href="https://www.emdat.be/" target="_blank">EM-DAT</a> |
<a href="https://prociv.gov.pt/" target="_blank">ANEPC</a>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<hr style="border: none; border-top: 1px solid #ccc; margin: 2em 0;">
""", unsafe_allow_html=True)



# === Parte 2 ===
st.markdown("<h2 style='text-align: center;'>Ocorrências Recentes (2024 - 2025) - Webscraping<br><span style='font-size: 0.8em; color: #777;'>(Recent Occurrences 2024 - 2025 - Webscraping)</span></h2>", unsafe_allow_html=True)



# ❗ Criar df_scraper_grouped ANTES de qualquer uso
df_scraper_grouped = df_scraper.groupby(["year", "month", "type"]).size().reset_index(name="ocorrencias")
df_scraper_grouped["data"] = pd.to_datetime(
    df_scraper_grouped["year"].astype(int).astype(str) + '-' +
    df_scraper_grouped["month"].astype(int).astype(str).str.zfill(2)
)

col4, col5, col6 = st.columns(3)

with col4:
    st.markdown(
    "<h4 style='text-align: center;'>Ocorrências agregadas por mês e tipologia<br><span style='font-size: 0.7em; color: #777;'>(Occurrences aggregated by month and type)</span></h4>",
    unsafe_allow_html=True
)

    st.dataframe(df_scraper_grouped, use_container_width=True)



with col5:
    st.markdown(
    "<h4 style='text-align: center;'>Ocorrências por distrito e tipo<br><span style='font-size: 0.7em; color: #777;'>(Occurrences by district and type)</span></h4>",
    unsafe_allow_html=True
)


    # Garantir nomes padronizados de distrito
    df_scraper["district"] = df_scraper["district"].astype(str).str.strip().str.title()
    df_scraper["district"] = df_scraper["district"].replace(substituir_distritos)

    # Agrupar por distrito e tipo
    df_distritos = df_scraper.groupby(["district", "type"]).size().reset_index(name="ocorrencias")

    # Remover distritos nulos ou vazios
    df_distritos = df_distritos[df_distritos["district"].notna() & (df_distritos["district"] != "")]

    # Ordenar para visualização clara
    distritos_ordenados = df_distritos.groupby("district")["ocorrencias"].sum().sort_values(ascending=False).index.tolist()

    chart_distritos = alt.Chart(df_distritos).mark_bar().encode(
        x=alt.X("district:N", title="Distrito", sort=distritos_ordenados),
        y=alt.Y("ocorrencias:Q", title="Ocorrências"),
        color=alt.Color(
            "type:N",
            scale=alt.Scale(domain=["Flood", "Landslide"], range=[COR_HEX["Flood"], COR_HEX["Landslide"]]),
            legend=alt.Legend(title="type")
        ),
        tooltip=["district", "type", "ocorrencias"]
    ).properties(
        height=400,
    )

    if df_distritos.empty:
        st.warning("Sem dados de ocorrências recentes por distrito.")
    else:
        st.altair_chart(chart_distritos, use_container_width=True)



with col6:
    st.markdown(
    "<h4 style='text-align: center;'>Ocorrências Recentes no Mapa (Scraper)<br><span style='font-size: 0.7em; color: #777;'>(Recent Occurrences on Map - Scraper)</span></h4>",
    unsafe_allow_html=True
)

    df_map = df_scraper.copy()

    tipo_mapa2_val = st.session_state.get("mapa2", "Todos")

    if tipo_mapa2_val != "Todos":
        df_map = df_map[df_map["type"] == tipo_mapa2_val]
        cor = COR_FLOOD if tipo_mapa2_val == "Flood" else COR_LANDSLIDE
        df_map["color"] = [cor] * len(df_map)
    else:
        df_map["color"] = df_map["type"].map({
            "Flood": COR_FLOOD,
            "Landslide": COR_LANDSLIDE
        })

    if not df_map.empty:
        # Uso de plotly map para mostrar ocorrências
        fig_map2 = px.scatter_map(
            df_map,
            lat="latitude",
            lon="longitude",
            color="type",
            hover_data=["district", "year", "month"],
            zoom=4.5,  
            height=400,
          
            center={"lat": 39.5, "lon": -8.0},  
            color_discrete_map={"Flood": COR_HEX["Flood"], "Landslide": COR_HEX["Landslide"]}
        )
        fig_map2.update_layout(
            mapbox_style="stamen-terrain",
            showlegend=True,
            margin={"r":0,"t":30,"l":0,"b":0}
        )
        st.plotly_chart(fig_map2, use_container_width=True)

        # Filtro abaixo do mapa
        tipo_mapa_2 = st.radio(
            "Selecionar tipo de desastre (mapa):",
            ["Todos", "Flood", "Landslide"],
            horizontal=True,
            key="mapa2"
        )
    else:
        st.warning("Sem dados de localização disponíveis.")



st.markdown("""
<div style='font-size: 0.9em; color: #555; margin-top: 1em; text-align: center;'>
<strong>Fontes:</strong> Jornais nacionais - 
<a href="https://news.google.com/rss" target="_blank">Google News RSS</a>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<hr style="border: none; border-top: 1px solid #ccc; margin: 2em 0;">
""", unsafe_allow_html=True)



# === Parte 3 ===
st.markdown("<h2 style='text-align: center;'>Previsão de Ocorrências para 2026<br><span style='font-size: 0.8em; color: #777;'>(Occurrence Predictions for 2026)</span></h2>", unsafe_allow_html=True)

# --- Enhanced Prediction with Time Series Analysis ---
def criar_features_temporais(df):
    """Criar características temporais melhoradas para melhores previsões"""
    df = df.copy()
    
    # Padrões sazonais para Portugal
    df['trimestre'] = ((df['month'] - 1) // 3) + 1
    df['estacao_chuvosa'] = df['month'].isin([10, 11, 12, 1, 2, 3]).astype(int)
    df['estacao_seca'] = df['month'].isin([6, 7, 8, 9]).astype(int)
    
    # Fatores de risco baseados no clima
    flood_risk_months = {12: 1.5, 1: 1.8, 2: 1.6, 3: 1.3, 4: 1.0, 5: 0.8, 
                        6: 0.4, 7: 0.3, 8: 0.3, 9: 0.6, 10: 1.0, 11: 1.2}
    landslide_risk_months = {12: 1.3, 1: 1.4, 2: 1.2, 3: 1.1, 4: 0.9, 5: 0.7,
                            6: 0.3, 7: 0.2, 8: 0.2, 9: 0.5, 10: 0.9, 11: 1.1}
    
    df['flood_seasonal_risk'] = df['month'].map(flood_risk_months)
    df['landslide_seasonal_risk'] = df['month'].map(landslide_risk_months)
    
    # Tendências temporais
    df['anos_desde_2000'] = df['year'] - 2000
    df['sin_month'] = np.sin(2 * np.pi * df['month'] / 12)
    df['cos_month'] = np.cos(2 * np.pi * df['month'] / 12)
    
    return df

def modelo_previsao_melhorado(df_historico):
    """Modelo de previsão melhorado com validação de séries temporais"""
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_absolute_error, r2_score
    
    previsoes_melhoradas = []
    modelos_validados = {}
    
    # Definir risco de inundação e deslizamento por mês - variável movida para dentro do escopo da função
    flood_risk_months = {12: 1.5, 1: 1.8, 2: 1.6, 3: 1.3, 4: 1.0, 5: 0.8, 
                        6: 0.4, 7: 0.3, 8: 0.3, 9: 0.6, 10: 1.0, 11: 1.2}
    landslide_risk_months = {
        1: 0.8, 2: 0.9, 3: 0.7, 4: 0.6, 5: 0.5, 6: 0.3,
        7: 0.2, 8: 0.2, 9: 0.4, 10: 0.6, 11: 0.7, 12: 0.8
    }
    
    for tipo in df_historico["type"].unique():
        df_tipo = df_historico[df_historico["type"] == tipo].copy()
        
        # Criar características melhoradas
        df_tipo = criar_features_temporais(df_tipo)
        
        # Preparar características
        feature_cols = ['month', 'anos_desde_2000', 'trimestre', 'estacao_chuvosa', 
                       'estacao_seca', 'sin_month', 'cos_month']
        
        if tipo == "Flood":
            feature_cols.append('flood_seasonal_risk')
        else:
            feature_cols.append('landslide_seasonal_risk')
        
        # Adicionar características de atraso histórico
        df_tipo = df_tipo.sort_values(['year', 'month'])
        df_tipo['lag_12'] = df_tipo['ocorrencias'].shift(12)  # Mesmo mês do ano anterior
        df_tipo['rolling_mean_6'] = df_tipo['ocorrencias'].rolling(6, min_periods=1).mean()
        df_tipo['rolling_std_6'] = df_tipo['ocorrencias'].rolling(6, min_periods=1).std().fillna(0)
        
        feature_cols.extend(['lag_12', 'rolling_mean_6', 'rolling_std_6'])
        
        # Remover linhas com valores NaN
        df_modelo = df_tipo.dropna(subset=feature_cols + ['ocorrencias'])
        
        if len(df_modelo) < 20:
            continue
        
        # Divisão de séries temporais (usar os últimos 2 anos para validação)
        cutoff_year = df_modelo['year'].max() - 2
        train_data = df_modelo[df_modelo['year'] <= cutoff_year]
        test_data = df_modelo[df_modelo['year'] > cutoff_year]
        
        if len(train_data) < 10 or len(test_data) < 5:
            # Recorrer a divisão aleatória se os dados forem insuficientes
            from sklearn.model_selection import train_test_split
            train_data, test_data = train_test_split(df_modelo, test_size=0.2, random_state=42)
        
        X_train = train_data[feature_cols]
        y_train = train_data['ocorrencias']
        X_test = test_data[feature_cols]
        y_test = test_data['ocorrencias']
        
        # Modelo Random Forest melhorado
        modelo = RandomForestRegressor(
            n_estimators=200,
            max_depth=10,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1
        )
        
        modelo.fit(X_train, y_train)
        
        # Validar modelo
        y_pred_test = modelo.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred_test)
        r2 = r2_score(y_test, y_pred_test)
        
        modelos_validados[tipo] = {
            'modelo': modelo,
            'features': feature_cols,
            'mae': mae,
            'r2': r2,
            'last_known_values': df_tipo.tail(12)  # Últimos 12 meses para contexto
        }
        
        # Gerar previsões para 2026
        meses_2026 = pd.date_range(start="2026-01", end="2026-12", freq="MS")
        
        for data in meses_2026:
            # Criar vetor de características para previsão
            features_pred = {
                'month': data.month,
                'anos_desde_2000': data.year - 2000,
                'trimestre': ((data.month - 1) // 3) + 1,
                'estacao_chuvosa': 1 if data.month in [10, 11, 12, 1, 2, 3] else 0,
                'estacao_seca': 1 if data.month in [6, 7, 8, 9] else 0,
                'sin_month': np.sin(2 * np.pi * data.month / 12),
                'cos_month': np.cos(2 * np.pi * data.month / 12)
            }
            
            if tipo == "Flood":
                features_pred['flood_seasonal_risk'] = flood_risk_months[data.month]
            else:
                features_pred['landslide_seasonal_risk'] = landslide_risk_months[data.month]
            
            # Estimar características de atraso com base em padrões históricos
            historical_same_month = df_tipo[df_tipo['month'] == data.month]['ocorrencias']
            if len(historical_same_month) > 0:
                features_pred['lag_12'] = historical_same_month.mean()
                features_pred['rolling_mean_6'] = historical_same_month.mean()
                features_pred['rolling_std_6'] = historical_same_month.std() if len(historical_same_month) > 1 else 0
            else:
                features_pred['lag_12'] = df_tipo['ocorrencias'].mean()
                features_pred['rolling_mean_6'] = df_tipo['ocorrencias'].mean()
                features_pred['rolling_std_6'] = df_tipo['ocorrencias'].std()
            
            # Criar array de características na ordem correta
            # X_pred = np.array([[features_pred[col] for col in feature_cols]])
            X_pred = pd.DataFrame([features_pred], columns=feature_cols)
            
            # Fazer previsão com incerteza
            prediction = modelo.predict(X_pred)[0]
            
            # Adicionar incerteza com base no desempenho do modelo
            uncertainty = mae * 1.5  # Estimativa conservadora de incerteza
            confidence_lower = max(0, prediction - uncertainty)
            confidence_upper = prediction + uncertainty
            
            previsoes_melhoradas.append({
                "data": data,
                "year": data.year,
                "month": data.month,
                "type": tipo,
                "ocorrencias": max(1, round(prediction)),
                "confidence_lower": max(0, round(confidence_lower)),
                "confidence_upper": round(confidence_upper),
                "model_r2": r2,
                "model_mae": mae
            })
    
    return pd.DataFrame(previsoes_melhoradas), modelos_validados

# Apply improved prediction
df_previsao_melhorada, info_modelos = modelo_previsao_melhorado(df_disasters)

# --- Layout das 3 colunas ---
col7, col8, col9 = st.columns(3)

with col7:
    st.markdown(
    "<h4 style='text-align: center;'>Previsões de Ocorrencias Mensais<br><span style='font-size: 0.7em; color: #777;'>(Monthly Occurrence Predictions)</span></h4>",
    unsafe_allow_html=True
)

    # Exibir previsões melhoradas com desempenho do modelo
    if not df_previsao_melhorada.empty:
        df_display = df_previsao_melhorada[['month', 'type', 'ocorrencias', 'confidence_lower', 'confidence_upper']].copy()
        df_display['intervalo_confianca'] = df_display['confidence_lower'].astype(str) + ' - ' + df_display['confidence_upper'].astype(str)
        df_display = df_display[['month', 'type', 'ocorrencias']]
        df_display.columns = ['Mês', 'Tipo', 'Previsão']
        st.dataframe(df_display, use_container_width=True)
    else:
        st.warning("Sem dados suficientes para previsão anual por distrito.")

with col8:
    st.markdown(
        "<h4 style='text-align: center;'>Previsão Anual por Distrito e Tipo<br><span style='font-size: 0.7em; color: #777;'>(Annual Prediction by District and Type)</span></h4>",
        unsafe_allow_html=True
    )

    if not df_previsao_melhorada.empty and not df_scraper.empty:
        # Obter distritos históricos por tipo
        distritos_tipo = df_scraper.groupby(['district', 'type']).size().reset_index(name='count')
        distritos_tipo = distritos_tipo[distritos_tipo['district'].notna() & (distritos_tipo['district'] != "")]
        distritos_tipo['district'] = distritos_tipo['district'].astype(str).str.strip().str.title()
        distritos_tipo['district'] = distritos_tipo['district'].replace(substituir_distritos)

        # Previsão anual total por tipo
        previsao_anual_tipo = df_previsao_melhorada.groupby('type')['ocorrencias'].sum().reset_index()

        # Distribuir previsões anuais pelos distritos históricos proporcionalmente
        distritos_tipo = distritos_tipo.merge(previsao_anual_tipo, on='type', how='left')
        distritos_tipo['ocorrencias_previstas'] = distritos_tipo['ocorrencias'] * distritos_tipo['count'] / distritos_tipo.groupby('type')['count'].transform('sum')
        distritos_tipo['ocorrencias_previstas'] = distritos_tipo['ocorrencias_previstas'].round().astype(int)

        # Gráfico de barras por distrito e tipo
        distritos_ordenados_prev = distritos_tipo.groupby("district")["ocorrencias_previstas"].sum().sort_values(ascending=False).index.tolist()
        chart_prev_distritos = alt.Chart(distritos_tipo).mark_bar().encode(
            x=alt.X("district:N", title="Distrito", sort=distritos_ordenados_prev),
            y=alt.Y("ocorrencias_previstas:Q", title="Ocorrências Previstas (2026)"),
            color=alt.Color(
                "type:N",
                scale=alt.Scale(domain=["Flood", "Landslide"], range=[COR_HEX["Flood"], COR_HEX["Landslide"]]),
                legend=alt.Legend(title="type")
            ),
            tooltip=["district", "type", "ocorrencias_previstas"]
        ).properties(
            height=400,
        )

        if distritos_tipo.empty:
            st.warning("Sem dados históricos de distritos para previsão.")
        else:
            st.altair_chart(chart_prev_distritos, use_container_width=True)
    else:
        st.warning("Sem dados suficientes para previsão anual por distrito.")

with col9:
    st.markdown(
        "<h4 style='text-align: center;'>Mapa de Previsões 2026<br><span style='font-size: 0.7em; color: #777;'>(2026 Predictions Map)</span></h4>",
        unsafe_allow_html=True
    )

    if not df_previsao_melhorada.empty and not df_scraper.empty:
        # Construir mapa com previsões proporcionais por distrito
        df_map_annual = distritos_tipo[["district", "type", "ocorrencias_previstas"]].copy()
        df_map_annual.rename(columns={"ocorrencias_previstas": "predicted_occurrences"}, inplace=True)

        # Coordenadas médias por distrito e tipo
        coord_por_distrito = df_scraper.groupby(["district", "type"]).agg({
            "latitude": "mean",
            "longitude": "mean"
        }).reset_index()

        df_map_annual = pd.merge(df_map_annual, coord_por_distrito, on=["district", "type"], how="left")
        df_map_annual = df_map_annual.dropna(subset=["latitude", "longitude"])

        # Usar o mesmo estilo de mapa que os outros mapas do dashboard
        fig_pred_map = px.scatter_map(
            df_map_annual,
            lat="latitude",
            lon="longitude",
            color="type",
            size="predicted_occurrences",
            hover_data=["district", "predicted_occurrences"],
            zoom=4.5,
            height=400,
            center={"lat": 39.5, "lon": -8.0},
            color_discrete_map={"Flood": COR_HEX["Flood"], "Landslide": COR_HEX["Landslide"]}
        )
        
        fig_pred_map.update_layout(
            mapbox_style="stamen-terrain",
            showlegend=True,
            margin={"r":0,"t":30,"l":0,"b":0}
        )

        max_occurrences = df_map_annual['predicted_occurrences'].max()
        min_occurrences = df_map_annual['predicted_occurrences'].min()
        
        st.plotly_chart(fig_pred_map, use_container_width=True)
        
        st.markdown(f"""
        **Nota:** As localizações são baseadas em padrões históricos.
        O tamanho dos círculos representa o número total de ocorrências previstas para 2026 (min: {min_occurrences}, max: {max_occurrences}).
        """)
    elif not df_previsao_melhorada.empty and df_scraper.empty:
        st.warning("Sem dados históricos de localização disponíveis.")
    else:
        st.warning("Sem previsões disponíveis para visualização no mapa.")



# --- Rodapé ---
st.markdown("---")

# Add the dashboard explanation section here at the bottom
st.markdown("""
<div style='background-color: #f8f9fa; border-radius: 12px; padding: 2em; margin-bottom: 2em; border: 1px solid #e9ecef;'>
    <h3 style='color: #2c3e50; margin-top: 0; text-align: center;'>Sobre o Dashboard
    <br><span style='font-size: 0.75em; color: #777; font-weight: normal;'>(About the Dashboard)</span></h3>
</div>
""", unsafe_allow_html=True)

# Use Streamlit columns instead of CSS grid for better compatibility
col_left, col_right = st.columns(2)

with col_left:
    st.markdown("""
    <div style='background-color: #ffffff; border-radius: 8px; padding: 1.5em; margin-bottom: 1em; border: 1px solid #e9ecef;'>
        <h4 style='color: #34495e; margin-bottom: 0.8em;'>Propósito</h4>
        <p style='margin-bottom: 1em; line-height: 1.6; font-size: 0.9em;'>
            Este dashboard apresenta uma análise abrangente de desastres naturais em Portugal, combinando dados históricos 
            com ocorrências recentes e previsões baseadas em machine learning para apoiar a tomada de decisões em gestão de riscos.
        </p>
        <p style='margin-bottom: 0; line-height: 1.6; font-size: 0.85em; color: #666; font-style: italic;'>
            Trabalho de âmbito académico, proposta na unidade curricular 21184 - Projeto de Engenharia Informática.
            <br><br>
            Autores: Luis Fernandes, Nuno Figueiredo, Paulo Couto, Rui Carvalho.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style='background-color: #ffffff; border-radius: 8px; padding: 1.5em; margin-bottom: 1em; border: 1px solid #e9ecef;'>
        <h4 style='color: #34495e; margin-bottom: 0.8em;'>Dados Históricos</h4>
        <p style='margin-bottom: 1em; line-height: 1.6; font-size: 0.9em;'>
            Compilação de múltiplas fontes científicas e institucionais:
        </p>
        <ul style='margin-left: 1em; font-size: 0.9em;'>
            <li><a href="https://idlcc.fc.ul.pt/pdf/Zezere_2014_DISASTER.pdf" target="_blank" style='color: #3498db;'>Disaster Database (Zêzere et al.)</a></li>
            <li><a href="https://eswd.eu/" target="_blank" style='color: #3498db;'>European Severe Weather Database</a></li>
            <li><a href="https://www.emdat.be/" target="_blank" style='color: #3498db;'>EM-DAT International Database</a></li>
            <li><a href="https://prociv.gov.pt/" target="_blank" style='color: #3498db;'>ANEPC - Autoridade Nacional de Emergência</a></li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

with col_right:
    st.markdown("""
    <div style='background-color: #ffffff; border-radius: 8px; padding: 1.5em; margin-bottom: 1em; border: 1px solid #e9ecef;'>
        <h4 style='color: #34495e; margin-bottom: 0.8em;'>Dados Recentes</h4>
        <p style='margin-bottom: 1em; line-height: 1.6; font-size: 0.9em;'>
            Ocorrências de 2024-2025 obtidas via webscraping de fontes noticiosas nacionais através do 
            <a href="https://news.google.com/rss" target="_blank" style='color: #3498db;'>Google News RSS</a>. 
            Os dados são validados contra bases existentes e georeferenciados automaticamente.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style='background-color: #ffffff; border-radius: 8px; padding: 1.5em; margin-bottom: 1em; border: 1px solid #e9ecef;'>
        <h4 style='color: #34495e; margin-bottom: 0.8em;'>Modelos Preditivos</h4>
        <p style='margin-bottom: 1em; line-height: 1.6; font-size: 0.9em;'>
            Previsões para 2026 utilizando algoritmos de Random Forest com características sazonais, 
            tendências temporais e padrões históricos específicos para Portugal.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style='background-color: #ffffff; border-radius: 8px; padding: 1.5em; margin-bottom: 1em; border: 1px solid #e9ecef;'>
        <h4 style='color: #34495e; margin-bottom: 0.8em;'>Tecnologias</h4>
        <p style='margin-bottom: 0; line-height: 1.6; font-size: 0.9em;'>
            <strong>Backend:</strong> Python, Scikit-learn, Pandas<br>
            <strong>Frontend:</strong> Streamlit, Plotly, Altair<br>
            <strong>Dados:</strong> Supabase, PostgreSQL<br>
            <strong>Deployment:</strong> Streamlit Cloud<br>
        </p>
    </div>
    """, unsafe_allow_html=True)

# Carregar dados das tabelas
df_disasters = carregar_disasters()
df_human_impacts = carregar_human_impacts()
df_location = carregar_localizacoes_disasters()
df_scraper = carregar_scraper()

# Tabelas adicionais
def carregar_information_sources():
    todos = []
    passo = 1000
    inicio = 0
    while True:
        response = supabase.table("information_sources").select("*").range(inicio, inicio + passo - 1).execute()
        dados = response.data
        if not dados:
            break
        todos.extend(dados)
        inicio += passo
    return pd.DataFrame(todos)

def carregar_spatial_ref_sys():
    todos = []
    passo = 1000
    inicio = 0
    while True:
        response = supabase.table("spatial_ref_sys").select("*").range(inicio, inicio + passo - 1).execute()
        dados = response.data
        if not dados:
            break
        todos.extend(dados)
        inicio += passo
    return pd.DataFrame(todos)

df_info_sources = carregar_information_sources()
df_spatial_ref = carregar_spatial_ref_sys()

# Gerar Excel em memória
excel_buffer = io.BytesIO()
with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
    df_disasters.to_excel(writer, sheet_name="disasters", index=False)
    df_human_impacts.to_excel(writer, sheet_name="human_impacts", index=False)
    df_location.to_excel(writer, sheet_name="location", index=False)
    df_scraper.to_excel(writer, sheet_name="google_scraper", index=False)
    df_info_sources.to_excel(writer, sheet_name="information_sources", index=False)
    df_spatial_ref.to_excel(writer, sheet_name="spatial_ref_sys", index=False)

excel_buffer.seek(0)

# Botão ao centro
st.markdown("""
<div style="text-align: center; margin-top: 3em;">
""", unsafe_allow_html=True)

st.download_button(
    label="Download dados",
    data=excel_buffer,
    file_name="simprede_dados_completos.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.markdown("</div>", unsafe_allow_html=True)