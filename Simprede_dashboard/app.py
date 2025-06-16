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

def get_base64_image(image_path):
    with open(image_path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode("utf-8")

logo_uab = get_base64_image("UAB.png")
logo_lei = get_base64_image("LEI.png")

# --- Configuração Supabase ---
url = "https://kyrfsylobmsdjlrrpful.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt5cmZzeWxvYm1zZGpscnJwZnVsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDUzNTY4MzEsImV4cCI6MjA2MDkzMjgzMX0.DkPGAw89OH6MPNnCvimfsVJICr5J9n9hcgdgF17cP34"
supabase: Client = create_client(url, key)

# --- Configuração da página ---
st.set_page_config(layout="wide", page_title="SIMPREDE", page_icon="🌍")


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
            <img src="data:image/png;base64,{logo_lei}" alt="Logotipo LEI">
        </div>
        <div class="title-text">
            <h1>SIMPREDE</h1>
            <h3>Sistema Inteligente de Monitorização e Previsão de Desastres Naturais</h3>
            <div class="subtitle-small">Universidade Aberta</div>
        </div>
        <div class="logo-img logo-right">
            <img src="data:image/png;base64,{logo_uab}" alt="Logotipo UAb">
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
st.markdown("<h2 style='text-align: center;'>Ocorrências Históricas de Desastres (1865 - 2025)</h2>", unsafe_allow_html=True)


# --- Agrupar dados históricos por mês/tipo ---
df_disasters_ano_tipo = df_disasters_raw.groupby(["year", "type"]).size().reset_index(name="ocorrencias")
df_disasters["date"] = pd.to_datetime(
    df_disasters["year"].astype(str) + "-" + df_disasters["month"].astype(str).str.zfill(2) + "-01"
)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
    "<h4 style='text-align: center;'>Dados históricos agregados por ano e tipologia</h4>",
    unsafe_allow_html=True
)

    df_ano_tipo = df_disasters_raw.groupby(["year", "type"]).size().reset_index(name="ocorrencias")
    st.dataframe(df_ano_tipo, use_container_width=True)

with col2:
    st.markdown(
    "<h4 style='text-align: center;'>Vítimas mortais por distrito e tipologia de desastre</h4>",
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
            legend=alt.Legend(title="Tipo de Desastre")
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
    "<h4 style='text-align: center;'>Ocorrências Históricas no Mapa</h4>",
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
            title="Ocorrências Históricas",
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
<div style='font-size: 0.9em; color: #555; margin-top: 1em;text-align: center;'>
<strong>Fontes:</strong> Disasters, ESWD, EMDAT e ANEPC
</div>
""", unsafe_allow_html=True)

st.markdown("""
<hr style="border: none; border-top: 1px solid #ccc; margin: 2em 0;">
""", unsafe_allow_html=True)



# === Parte 2 ===
st.markdown("<h2 style='text-align: center;'>Ocorrências Recentes (2024 - 2025) - Webscraping</h2>", unsafe_allow_html=True)



# ❗ Criar df_scraper_grouped ANTES de qualquer uso
df_scraper_grouped = df_scraper.groupby(["year", "month", "type"]).size().reset_index(name="ocorrencias")
df_scraper_grouped["data"] = pd.to_datetime(
    df_scraper_grouped["year"].astype(int).astype(str) + '-' +
    df_scraper_grouped["month"].astype(int).astype(str).str.zfill(2)
)

col4, col5, col6 = st.columns(3)

with col4:
    st.markdown(
    "<h4 style='text-align: center;'>Ocorrências agregadas por mês e tipologia</h4>",
    unsafe_allow_html=True
)

    st.dataframe(df_scraper_grouped, use_container_width=True)



with col5:
    st.markdown(
    "<h4 style='text-align: center;'>Ocorrências por distrito e tipo</h4>",
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
            legend=alt.Legend(title="Tipo")
        ),
        tooltip=["district", "type", "ocorrencias"]
    ).properties(
        height=400,
        title="Distribuição de Ocorrências por Distrito"
    )

    if df_distritos.empty:
        st.warning("Sem dados de ocorrências recentes por distrito.")
    else:
        st.altair_chart(chart_distritos, use_container_width=True)



with col6:
    st.markdown(
    "<h4 style='text-align: center;'>Ocorrências Recentes no Mapa (Scraper)</h4>",
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
            title="Ocorrências Recentes (Scraper)",
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
<div style='font-size: 0.9em; color: #555; margin-top: 1em;text-align: center;'>
<strong>Fontes:</strong> Jornais nacionais - Google News 
</div>
""", unsafe_allow_html=True)

st.markdown("""
<hr style="border: none; border-top: 1px solid #ccc; margin: 2em 0;">
""", unsafe_allow_html=True)



# === Parte 3 ===
st.markdown("<h2 style='text-align: center;'>Previsão de Ocorrências para 2026</h2>", unsafe_allow_html=True)



# --- Previsão com RandomForestRegressor por tipo ---
previsoes = []
meses_futuros = pd.date_range(start="2026-01", end="2026-12", freq="MS")

for tipo in df_disasters["type"].unique():
    df_tipo = df_disasters[df_disasters["type"] == tipo]
    X = df_tipo[["year", "month"]]
    y = df_tipo["ocorrencias"]

    modelo = LinearRegression()

    modelo.fit(X, y)

    df_futuro = pd.DataFrame({"year": meses_futuros.year, "month": meses_futuros.month})
    y_pred = modelo.predict(df_futuro[["year", "month"]])

    for i, data in enumerate(meses_futuros):
        previsoes.append({
            "data": data,
            "year": data.year,
            "month": data.month,
            "type": tipo,
            "ocorrencias": max(0, round(y_pred[i]))
        })

df_prev = pd.DataFrame(previsoes)



# --- Criar previsões geográficas por município ---
df_completo = pd.merge(df_disasters_raw, df_loc_disasters, on="id").dropna(subset=["year", "month"])
df_historico_mun = df_completo.groupby(["year", "month", "type", "municipality", "latitude", "longitude"]).size().reset_index(name="ocorrencias")

previsoes_geo = []

for (municipio, tipo), grupo in df_historico_mun.groupby(["municipality", "type"]):
    if len(grupo["year"].unique()) >= 2:
        X = grupo[["year", "month"]]
        y = grupo["ocorrencias"]

        modelo = LinearRegression()
        modelo.fit(X, y)

        df_futuro = pd.DataFrame({"year": meses_futuros.year, "month": meses_futuros.month})
        y_pred = modelo.predict(df_futuro[["year", "month"]])

        for i, data in enumerate(meses_futuros):
            previsoes_geo.append({
                "data": data,
                "year": data.year,
                "month": data.month,
                "type": tipo,
                "municipality": municipio,
                "ocorrencias": max(0, round(y_pred[i])),
                "latitude": grupo["latitude"].iloc[0],
                "longitude": grupo["longitude"].iloc[0]
            })

df_previsao_mapa = pd.DataFrame(previsoes_geo)


# --- Layout das 3 colunas ---
col7, col8, col9 = st.columns(3)

with col7:
    st.markdown(
    "<h4 style='text-align: center;'>Dados de Previsão</h4>",
    unsafe_allow_html=True
)

    st.dataframe(df_prev, use_container_width=True)

with col8:
    st.markdown(
    "<h4 style='text-align: center;'>Previsão de Ocorrências por Distrito (2026)</h4>",
    unsafe_allow_html=True
)


    # Juntar previsão com distritos corretos via merge
    df_prev_distritos = pd.merge(
        df_previsao_mapa,
        df_loc_disasters[["municipality", "district"]],
        on="municipality",
        how="left"
    )

    # Padronizar nomes dos distritos
    df_prev_distritos["district"] = df_prev_distritos["district"].astype(str).str.strip().str.title()
    df_prev_distritos["district"] = df_prev_distritos["district"].replace(substituir_distritos)

    # Agrupar por distrito e tipo
    df_prev_distritos = df_prev_distritos.groupby(["district", "type"])["ocorrencias"].sum().reset_index()

    # Filtrar apenas distritos válidos
    distritos_validos = list(substituir_distritos.values())
    df_prev_distritos = df_prev_distritos[df_prev_distritos["district"].isin(distritos_validos)]

    # Ordenar distritos pelo total
    ordenados = df_prev_distritos.groupby("district")["ocorrencias"].sum().sort_values(ascending=False).index.tolist()

    chart_prev_distritos = alt.Chart(df_prev_distritos).mark_bar().encode(
        x=alt.X("district:N", title=None, sort=ordenados),
        y=alt.Y("ocorrencias:Q", title="Previsão Total de Ocorrências (2026)"),
        color=alt.Color(
            "type:N",
            scale=alt.Scale(domain=["Flood", "Landslide"], range=[COR_HEX["Flood"], COR_HEX["Landslide"]]),
            legend=alt.Legend(title="Tipo")
        ),
        tooltip=["district", "type", "ocorrencias"]
    ).properties(
        height=400,
        
    )

    if df_prev_distritos.empty:
        st.warning("Sem dados previstos por distrito.")
    else:
        st.altair_chart(chart_prev_distritos, use_container_width=True)

# 🔁 Criar dataframe de previsão agregada por distrito (para usar no mapa)
df_prev_distritos_mapa = pd.merge(
    df_previsao_mapa,
    df_loc_disasters[["municipality", "district", "latitude", "longitude"]],
    on="municipality",
    how="left"
)

with col9:
    st.markdown(
    "<h4 style='text-align: center;'>Mapa de Ocorrências Previstas (2026)</h4>",
    unsafe_allow_html=True
)


    df_vis = df_previsao_mapa.copy()

    # Esta linha fica fora do .radio() para ser atualizada abaixo
    tipo_prev_mapa_val = st.session_state.get("mapa_prev_simples_bottom", "Todos")

    # Filtrar dados ANTES de desenhar o mapa
    if tipo_prev_mapa_val != "Todos":
        df_vis = df_vis[df_vis["type"] == tipo_prev_mapa_val]

    if not df_vis.empty:
        # Mostrar o mapa primeiro
        st.pydeck_chart(pdk.Deck(
            initial_view_state=pdk.ViewState(
                latitude=df_vis["latitude"].mean(),
                longitude=df_vis["longitude"].mean(),
                zoom=5
            ),
            layers=[
                pdk.Layer(
                    "HeatmapLayer",
                    data=df_vis,
                    get_position='[longitude, latitude]',
                    get_weight='ocorrencias',
                    aggregation="SUM",
                    pickable=True
                )
            ],
            map_style="mapbox://styles/mapbox/light-v9"
        ), height=400)

        # Agora mostramos o filtro por tipo de desastre (DEPOIS do mapa)
        tipo_prev_mapa = st.radio(
            "Selecionar tipo de desastre (mapa):",
            ["Todos", "Flood", "Landslide"],
            horizontal=True,
            key="mapa_prev_simples_bottom"
        )

    else:
        st.warning("Sem dados de previsão disponíveis para o mapa.")
        
        
st.markdown("""
<div style='font-size: 0.9em; color: #555; margin-top: 1em;text-align: center;'>
<strong>Fontes:</strong> Disasters, ESWD, EMDAT e ANEPC
</div>
""", unsafe_allow_html=True)

st.markdown("""
<hr style="border: none; border-top: 1px solid #ccc; margin: 2em 0;">
""", unsafe_allow_html=True)


# Rodapé
st.markdown("---")
st.caption("Projeto de Engenharia Informática<br>Autores: Luis Fernandes, Nuno Figueiredo, Paulo Couto, Rui Carvalho.", unsafe_allow_html=True)

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