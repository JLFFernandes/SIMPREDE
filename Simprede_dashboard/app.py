import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import altair as alt
import pydeck as pdk
import io
import base64
from datetime import datetime
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
import numpy as np
from utils.supabase_connector import SupabaseConnection

# Page configuration
st.set_page_config(
    page_title="SIMPREDE",
    page_icon="🌍",
    layout="wide"
)

# Initialize connection
@st.cache_resource
def init_connection():
    try:
        return SupabaseConnection()
    except ValueError as e:
        st.error("❌ Erro de configuração da base de dados")
        st.error(str(e))
        st.info("ℹ️ Por favor, configure as credenciais do Supabase nas configurações da aplicação.")
        st.stop()
    except Exception as e:
        st.error("❌ Erro inesperado ao conectar à base de dados")
        st.error(str(e))
        st.stop()

# Helper functions for image encoding
def get_base64_image(image_path):
    try:
        with open(image_path, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode("utf-8")
    except FileNotFoundError:
        st.warning(f"Image file {image_path} not found")
        return ""

# Load logo images
logo_uab = get_base64_image("UAB.png")
logo_lei = get_base64_image("LEI.png")

# Global color definitions
COR_HEX = {
    "Flood": "#1f77b4",
    "Landslide": "#ff7f0e"
}
COR_RGBA = {
    "Flood": [31, 119, 180, 160],
    "Landslide": [255, 127, 14, 160]
}

# Custom CSS styling
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

# Load data with caching
@st.cache_data(ttl=300)
def load_ocorrencias_data():
    conn = init_connection()
    df = conn.get_ocorrencias_data()
    if not df.empty:
        if 'type' in df.columns:
            df = df[df['type'] != 'Other']
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])
        if 'date' in df.columns:
            try:
                df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
            except ValueError:
                df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
    return df

@st.cache_data(ttl=300)
def load_location_data():
    conn = init_connection()
    df = conn.get_location_data()
    if not df.empty and 'type' in df.columns:
        df = df[df['type'] != 'Other']
    return df

@st.cache_data(ttl=300)
def load_eswd_data():
    conn = init_connection()
    df = conn.get_eswd_data()
    if not df.empty:
        if 'date' in df.columns:
            try:
                df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
            except ValueError:
                df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
    return df

# New functions that match app_new.py structure but use existing backend
@st.cache_data(ttl=300)
def carregar_disasters():
    """Load disasters data using existing backend"""
    df = load_eswd_data()
    if not df.empty:
        # Ensure columns are properly formatted
        df["type"] = df["type"].str.capitalize()
        df = df[df["type"].isin(["Flood", "Landslide"])]
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df["month"] = pd.to_numeric(df["month"], errors="coerce")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.dropna(subset=["year", "month", "date"]) if not df.empty else df

@st.cache_data(ttl=300)
def carregar_localizacoes_disasters():
    """Load location data using existing backend"""
    df = load_eswd_data()
    if not df.empty:
        df = df.dropna(subset=["latitude", "longitude"])
    return df

@st.cache_data(ttl=300)
def carregar_scraper():
    """Load scraper data using existing backend"""
    df = load_ocorrencias_data()
    if not df.empty:
        df["type"] = df["type"].str.capitalize()
        df = df[df["type"].isin(["Flood", "Landslide"])]
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df["month"] = pd.to_numeric(df["month"], errors="coerce")
        df = df.dropna(subset=["year", "month", "latitude", "longitude"])
    return df

@st.cache_data(ttl=300)
def carregar_human_impacts():
    """Load human impacts data using existing backend"""
    df = load_eswd_data()
    # Filter for human impact columns
    impact_cols = ["id", "fatalities", "injured", "evacuated"]
    available_cols = [col for col in impact_cols if col in df.columns]
    return df[available_cols] if not df.empty and available_cols else pd.DataFrame()

# District name corrections dictionary
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
}

# Helper function to safely get unique values excluding None
def get_unique_values(series, exclude_none=True):
    """Get unique values from a pandas Series, optionally excluding None values"""
    if exclude_none:
        return sorted([x for x in series.unique() if x is not None and pd.notna(x)])
    else:
        return sorted(series.unique())

# Load all data
df_disasters_raw = carregar_disasters()
df_scraper = carregar_scraper()
df_loc_disasters = carregar_localizacoes_disasters()
df_human_impacts = carregar_human_impacts()

# Apply district name corrections to location data 
if not df_loc_disasters.empty and "district" in df_loc_disasters.columns:
    df_loc_disasters["district"] = df_loc_disasters["district"].str.strip().str.title()
    df_loc_disasters["district"] = df_loc_disasters["district"].replace(substituir_distritos)

# Process disasters data
if not df_disasters_raw.empty:
    df_disasters = df_disasters_raw.groupby(["year", "month", "type"]).size().reset_index(name="ocorrencias")
    df_disasters["date"] = pd.to_datetime(
        df_disasters["year"].astype(str) + "-" + df_disasters["month"].astype(str).str.zfill(2) + "-01"
    )
else:
    df_disasters = pd.DataFrame()

# Process scraper data
if not df_scraper.empty:
    df_scraper_grouped = df_scraper.groupby(["year", "month", "type"]).size().reset_index(name="ocorrencias")
    df_scraper_grouped["data"] = pd.to_datetime(
        df_scraper_grouped["year"].astype(int).astype(str) + '-' +
        df_scraper_grouped["month"].astype(int).astype(str).str.zfill(2)
    )
else:
    df_scraper_grouped = pd.DataFrame()

# === SECTION 1: Historical Disasters (1865 - 2025) ===
st.markdown("<h2 style='text-align: center;'>Ocorrências Históricas de Desastres (1865 - 2025)</h2>", unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        "<h4 style='text-align: center;'>Dados históricos agregados por ano e tipologia</h4>",
        unsafe_allow_html=True
    )
    
    if not df_disasters_raw.empty:
        df_ano_tipo = df_disasters_raw.groupby(["year", "type"]).size().reset_index(name="ocorrencias")
        st.dataframe(df_ano_tipo, use_container_width=True)
    else:
        st.warning("Sem dados históricos disponíveis")

with col2:
    st.markdown(
        "<h4 style='text-align: center;'>Vítimas mortais por distrito e tipologia de desastre</h4>",
        unsafe_allow_html=True
    )
    
    if not df_disasters_raw.empty:
        # Since all data is in the main table, no merge needed
        df_for_fatalities = df_disasters_raw.copy()
        
        # Clean data only if columns exist
        if "fatalities" in df_for_fatalities.columns:
            df_for_fatalities["fatalities"] = pd.to_numeric(df_for_fatalities["fatalities"], errors="coerce").fillna(0)
        else:
            df_for_fatalities["fatalities"] = 0
            
        if "district" in df_for_fatalities.columns:
            df_for_fatalities["district"] = df_for_fatalities["district"].astype(str).str.strip().str.title()
            df_for_fatalities["district"] = df_for_fatalities["district"].replace(substituir_distritos)
        
        if "type" in df_for_fatalities.columns:
            df_for_fatalities["type"] = df_for_fatalities["type"].str.capitalize()
        
        # Group by district and type only if we have the required columns
        if "district" in df_for_fatalities.columns and "type" in df_for_fatalities.columns and "fatalities" in df_for_fatalities.columns:
            # Remove null districts
            df_for_fatalities = df_for_fatalities[df_for_fatalities["district"].notna() & (df_for_fatalities["district"] != "")]
            
            # Group by district and type
            df_grouped = df_for_fatalities.groupby(["district", "type"])["fatalities"].sum().reset_index()
            df_grouped = df_grouped[df_grouped["fatalities"] > 0]
        else:
            df_grouped = pd.DataFrame()
        
        if not df_grouped.empty:
            # Scale Y axis with 10% margin
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
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.warning("Sem dados de vítimas mortais disponíveis.")
    else:
        st.warning("Dados insuficientes para análise de vítimas mortais")

with col3:
    st.markdown(
        "<h4 style='text-align: center;'>Ocorrências Históricas no Mapa</h4>",
        unsafe_allow_html=True
    )
    
    if not df_disasters_raw.empty:
        # No merge needed - all data is in the main table
        df_for_map = df_disasters_raw.copy()
        
        # Check if coordinate columns exist
        if "latitude" in df_for_map.columns and "longitude" in df_for_map.columns:
            # Filter out rows with invalid coordinates
            df_for_map = df_for_map.dropna(subset=["latitude", "longitude"])
            df_for_map = df_for_map[
                (df_for_map["latitude"].between(-90, 90)) & 
                (df_for_map["longitude"].between(-180, 180))
            ]
            
            if not df_for_map.empty:
                # Map filter
                tipo_mapa1_val = st.selectbox("Filtrar por tipo:", ["Todos", "Flood", "Landslide"], key="mapa1")
                
                if tipo_mapa1_val != "Todos":
                    df_for_map = df_for_map[df_for_map["type"] == tipo_mapa1_val]
                    cor = COR_RGBA["Flood"] if tipo_mapa1_val == "Flood" else COR_RGBA["Landslide"]
                    df_for_map["color"] = [cor] * len(df_for_map)
                else:
                    df_for_map["color"] = df_for_map["type"].map({
                        "Flood": COR_RGBA["Flood"],
                        "Landslide": COR_RGBA["Landslide"]
                    })
                
                if not df_for_map.empty:
                    st.pydeck_chart(pdk.Deck(
                        initial_view_state=pdk.ViewState(
                            latitude=df_for_map["latitude"].mean(),
                            longitude=df_for_map["longitude"].mean(),
                            zoom=5.0
                        ),
                        layers=[pdk.Layer(
                            "ScatterplotLayer",
                            data=df_for_map.to_dict(orient="records"),
                            get_position='[longitude, latitude]',
                            get_radius=8000,
                            get_color='color',
                            pickable=True
                        )],
                        map_style="mapbox://styles/mapbox/light-v9"
                    ), height=400)
                else:
                    st.warning("Sem dados válidos após filtros")
            else:
                st.warning("Sem dados com coordenadas válidas para o mapa")
        else:
            st.warning("Coordenadas geográficas não encontradas nos dados")
    else:
        st.warning("Dados insuficientes para o mapa")

# Sources and separator
st.markdown("""
<div style='font-size: 0.9em; color: #555; margin-top: 1em;text-align: center;'>
<strong>Fontes:</strong> Disasters, ESWD, EMDAT e ANEPC
</div>
""", unsafe_allow_html=True)

st.markdown("""
<hr style="border: none; border-top: 1px solid #ccc; margin: 2em 0;">
""", unsafe_allow_html=True)

# === SECTION 2: Recent Events (2024 - 2025) - Webscraping ===
st.markdown("<h2 style='text-align: center;'>Ocorrências Recentes (2024 - 2025) - Webscraping</h2>", unsafe_allow_html=True)

col4, col5, col6 = st.columns(3)

with col4:
    st.markdown(
        "<h4 style='text-align: center;'>Ocorrências agregadas por mês e tipologia</h4>",
        unsafe_allow_html=True
    )
    
    if not df_scraper_grouped.empty:
        st.dataframe(df_scraper_grouped, use_container_width=True)
    else:
        st.warning("Sem dados de scraper disponíveis")

with col5:
    st.markdown(
        "<h4 style='text-align: center;'>Ocorrências por distrito e tipo</h4>",
        unsafe_allow_html=True
    )
    
    if not df_scraper.empty:
        # Check if required columns exist
        required_cols = ["district", "type"]
        if all(col in df_scraper.columns for col in required_cols):
            # Standardize district names
            df_scraper["district"] = df_scraper["district"].astype(str).str.strip().str.title()
            df_scraper["district"] = df_scraper["district"].replace(substituir_distritos)
            
            # Group by district and type
            df_distritos = df_scraper.groupby(["district", "type"]).size().reset_index(name="ocorrencias")
            
            # Remove null districts
            df_distritos = df_distritos[df_distritos["district"].notna() & (df_distritos["district"] != "")]
        else:
            df_distritos = pd.DataFrame()
        
        if not df_distritos.empty:
            # Order for clear visualization
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
            
            st.altair_chart(chart_distritos, use_container_width=True)
        else:
            st.warning("Sem dados válidos por distrito")
    else:
        st.warning("Sem dados de scraper disponíveis")

with col6:
    st.markdown(
        "<h4 style='text-align: center;'>Ocorrências Recentes no Mapa (Scraper)</h4>",
        unsafe_allow_html=True
    )
    
    if not df_scraper.empty:
        # Check if required columns exist
        required_cols = ["latitude", "longitude", "type"]
        if all(col in df_scraper.columns for col in required_cols):
            df_map = df_scraper.copy()
            
            # Double-check that latitude and longitude actually exist
            if "latitude" in df_map.columns and "longitude" in df_map.columns:
                # Filter out rows with invalid coordinates
                df_map = df_map.dropna(subset=["latitude", "longitude"])
                df_map = df_map[
                    (df_map["latitude"].between(-90, 90)) & 
                    (df_map["longitude"].between(-180, 180))
                ]
            else:
                st.warning("Coordenadas geográficas não encontradas nos dados do scraper")
                df_map = pd.DataFrame()  # Empty dataframe if no coordinates
            
            if not df_map.empty:
                # Map filter
                tipo_mapa2_val = st.selectbox("Filtrar por tipo:", ["Todos", "Flood", "Landslide"], key="mapa2")
                
                if tipo_mapa2_val != "Todos":
                    df_map = df_map[df_map["type"] == tipo_mapa2_val]
                    cor = COR_RGBA["Flood"] if tipo_mapa2_val == "Flood" else COR_RGBA["Landslide"]
                    df_map["color"] = [cor] * len(df_map)
                else:
                    df_map["color"] = df_map["type"].map({
                        "Flood": COR_RGBA["Flood"],
                        "Landslide": COR_RGBA["Landslide"]
                    })
                
                if not df_map.empty:
                    st.pydeck_chart(pdk.Deck(
                        initial_view_state=pdk.ViewState(
                            latitude=df_map["latitude"].mean(),
                            longitude=df_map["longitude"].mean(),
                            zoom=5.0
                        ),
                        layers=[pdk.Layer(
                            "ScatterplotLayer",
                            data=df_map.to_dict(orient="records"),
                            get_position='[longitude, latitude]',
                            get_radius=8000,
                            get_color='color',
                            pickable=True
                        )],
                        map_style="mapbox://styles/mapbox/light-v9"
                    ), height=400)
                else:
                    st.warning("Sem dados válidos após filtros")
            else:
                st.warning("Sem dados com coordenadas válidas para o mapa")
        else:
            st.warning("Colunas necessárias não encontradas para o mapa")
    else:
        st.warning("Sem dados de scraper disponíveis")

# Sources and separator
st.markdown("""
<div style='font-size: 0.9em; color: #555; margin-top: 1em;text-align: center;'>
<strong>Fontes:</strong> Jornais nacionais - Google News 
</div>
""", unsafe_allow_html=True)

st.markdown("""
<hr style="border: none; border-top: 1px solid #ccc; margin: 2em 0;">
""", unsafe_allow_html=True)

# === SECTION 3: Predictions for 2026 ===
st.markdown("<h2 style='text-align: center;'>Previsão de Ocorrências para 2026</h2>", unsafe_allow_html=True)

# Generate predictions using a more sensible approach
previsoes = []
meses_futuros = pd.date_range(start="2026-01", end="2026-12", freq="MS")

if not df_disasters.empty and "type" in df_disasters.columns:
    for tipo in df_disasters["type"].unique():
        df_tipo = df_disasters[df_disasters["type"] == tipo]
        required_cols = ["year", "month", "ocorrencias"]
        
        if len(df_tipo) > 12 and all(col in df_tipo.columns for col in required_cols):  # Need at least 1 year of data
            try:
                # Create a time-based feature instead of just year/month
                df_tipo = df_tipo.sort_values(['year', 'month'])
                df_tipo['time_index'] = range(len(df_tipo))
                
                # Use time index as feature (more sensible for time series)
                X = df_tipo[['time_index']].values
                y = df_tipo['ocorrencias'].values
                
                # Simple linear regression on time trend
                modelo = LinearRegression()
                modelo.fit(X, y)
                
                # Predict for 2026 (12 months ahead from the last data point)
                last_time_index = df_tipo['time_index'].max()
                
                for i, data in enumerate(meses_futuros):
                    future_time_index = last_time_index + i + 1
                    y_pred = modelo.predict(np.array([[future_time_index]]))
                    
                    # Use historical average as baseline and add trend
                    historical_avg = df_tipo['ocorrencias'].mean()
                    predicted_value = max(0, round(y_pred[0]))
                    
                    # Cap prediction at reasonable bounds (no more than 3x historical max)
                    historical_max = df_tipo['ocorrencias'].max()
                    predicted_value = min(predicted_value, historical_max * 3)
                    
                    previsoes.append({
                        "data": data.strftime("%Y-%m"),
                        "type": tipo,
                        "ocorrencias": predicted_value
                    })
            except Exception as e:
                # If prediction fails, use simple seasonal average
                try:
                    # Calculate seasonal average (by month)
                    seasonal_avg = df_tipo.groupby('month')['ocorrencias'].mean()
                    
                    for i, data in enumerate(meses_futuros):
                        month = data.month
                        avg_for_month = seasonal_avg.get(month, df_tipo['ocorrencias'].mean())
                        
                        previsoes.append({
                            "data": data.strftime("%Y-%m"),
                            "type": tipo,
                            "ocorrencias": max(1, round(avg_for_month))
                        })
                except:
                    # Final fallback: use overall average
                    overall_avg = max(1, round(df_tipo['ocorrencias'].mean()))
                    for i, data in enumerate(meses_futuros):
                        previsoes.append({
                            "data": data.strftime("%Y-%m"),
                            "type": tipo,
                            "ocorrencias": overall_avg
                        })

df_prev = pd.DataFrame(previsoes)

# Generate geographic predictions using simpler approach
previsoes_geo = []

if not df_disasters_raw.empty:
    # Since all location data is in the main table, no merge needed
    df_completo = df_disasters_raw.copy()
    
    # Check if required columns exist
    required_cols = ["year", "type", "municipality", "latitude", "longitude"]
    
    if all(col in df_completo.columns for col in required_cols):
        # Filter out invalid coordinates
        df_completo = df_completo.dropna(subset=["latitude", "longitude"])
        df_completo = df_completo[
            (df_completo["latitude"].between(-90, 90)) & 
            (df_completo["longitude"].between(-180, 180))
        ]
        
        if not df_completo.empty:
            try:
                # Calculate historical averages by municipality and type
                # This is more reliable than complex regression for sparse data
                df_historico_mun = df_completo.groupby(["year", "type", "municipality", "latitude", "longitude"]).size().reset_index(name="ocorrencias")
                
                # Calculate average occurrences per year for each municipality-type combination
                municipality_averages = df_historico_mun.groupby(["municipality", "type"]).agg({
                    "ocorrencias": "mean",
                    "latitude": "first",
                    "longitude": "first"
                }).reset_index()
                
                # Only include municipalities with meaningful historical data
                municipality_averages = municipality_averages[municipality_averages["ocorrencias"] >= 0.5]
                
                for _, row in municipality_averages.iterrows():
                    # Predict based on historical average with some variability
                    historical_avg = row["ocorrencias"]
                    predicted_value = max(1, round(historical_avg))
                    
                    previsoes_geo.append({
                        "municipality": row["municipality"],
                        "type": row["type"],
                        "ocorrencias": predicted_value,
                        "latitude": row["latitude"],
                        "longitude": row["longitude"]
                    })
                    
            except Exception as e:
                st.warning(f"Erro ao gerar previsões geográficas: {str(e)}")

df_previsao_mapa = pd.DataFrame(previsoes_geo)

col7, col8, col9 = st.columns(3)

with col7:
    st.markdown(
        "<h4 style='text-align: center;'>Dados de Previsão</h4>",
        unsafe_allow_html=True
    )
    
    if not df_prev.empty:
        st.dataframe(df_prev, use_container_width=True)
    else:
        st.warning("Sem dados suficientes para previsão")

with col8:
    st.markdown(
        "<h4 style='text-align: center;'>Previsão de Ocorrências por Distrito (2026)</h4>",
        unsafe_allow_html=True
    )
    
    if not df_previsao_mapa.empty:
        # Since all location data is in the main disasters table, we can directly use districts
        # Join predictions with districts from the same table
        df_prev_distritos = df_previsao_mapa.copy()
        
        # Get district information for each municipality from the disasters table
        municipality_to_district = df_disasters_raw.groupby("municipality")["district"].first().to_dict()
        df_prev_distritos["district"] = df_prev_distritos["municipality"].map(municipality_to_district)
        
        # Standardize district names
        df_prev_distritos["district"] = df_prev_distritos["district"].astype(str).str.strip().str.title()
        df_prev_distritos["district"] = df_prev_distritos["district"].replace(substituir_distritos)
        
        # Group by district and type
        df_prev_distritos = df_prev_distritos.groupby(["district", "type"])["ocorrencias"].sum().reset_index()
        
        # Filter only valid districts (remove NaN and empty)
        df_prev_distritos = df_prev_distritos[df_prev_distritos["district"].notna() & (df_prev_distritos["district"] != "nan")]
        
        if not df_prev_distritos.empty:
            # Order districts by total
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
                title="Previsões por Distrito"
            )
            
            st.altair_chart(chart_prev_distritos, use_container_width=True)
        else:
            st.warning("Sem previsões válidas por distrito")
    else:
        st.warning("Dados insuficientes para previsão por distrito")

with col9:
    st.markdown(
        "<h4 style='text-align: center;'>Mapa de Ocorrências Previstas (2026)</h4>",
        unsafe_allow_html=True
    )
    
    if not df_previsao_mapa.empty:
        # Create district-level aggregated data for the map with representative coordinates
        df_map_data = df_previsao_mapa.copy()
        
        # Get district information for each municipality from the disasters table
        municipality_to_district = df_disasters_raw.groupby("municipality")["district"].first().to_dict()
        df_map_data["district"] = df_map_data["municipality"].map(municipality_to_district)
        
        # Standardize district names
        df_map_data["district"] = df_map_data["district"].astype(str).str.strip().str.title()
        df_map_data["district"] = df_map_data["district"].replace(substituir_distritos)
        
        # Remove invalid districts
        df_map_data = df_map_data[df_map_data["district"].notna() & (df_map_data["district"] != "nan")]
        
        if not df_map_data.empty:
            # Calculate representative coordinates for each district (centroid)
            district_coords = df_map_data.groupby("district").agg({
                "latitude": "mean",
                "longitude": "mean"
            }).reset_index()
            
            # Aggregate predictions by district and type
            district_predictions = df_map_data.groupby(["district", "type"])["ocorrencias"].sum().reset_index()
            
            # Merge coordinates with predictions
            df_vis_base = district_predictions.merge(district_coords, on="district", how="left")
            
            # Get the filter value from session state, default to "Todos"
            tipo_prev_mapa_val = st.session_state.get("mapa_prev_simples_bottom", "Todos")
            
            # Filter data BEFORE drawing the map
            if tipo_prev_mapa_val != "Todos":
                df_vis = df_vis_base[df_vis_base["type"] == tipo_prev_mapa_val]
            else:
                df_vis = df_vis_base.copy()
            
            if not df_vis.empty:
                # Show the map first
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
                
                # Now show the filter for disaster type (AFTER the map)
                tipo_prev_mapa = st.radio(
                    "Selecionar tipo de desastre (mapa):",
                    ["Todos", "Flood", "Landslide"],
                    horizontal=True,
                    key="mapa_prev_simples_bottom"
                )
            else:
                st.warning("Sem dados válidos após filtros")
        else:
            st.warning("Sem dados de distrito válidos para o mapa")
    else:
        st.warning("Sem dados de previsão disponíveis para o mapa")

# Sources and separator
st.markdown("""
<div style='font-size: 0.9em; color: #555; margin-top: 1em;text-align: center;'>
<strong>Fontes:</strong> Disasters, ESWD, EMDAT e ANEPC
</div>
""", unsafe_allow_html=True)

st.markdown("""
<hr style="border: none; border-top: 1px solid #ccc; margin: 2em 0;">
""", unsafe_allow_html=True)

# Footer
st.markdown("---")
st.caption("Projeto de Engenharia Informática<br>Autores: Luis Fernandes, Nuno Figueiredo, Paulo Couto, Rui Carvalho.", unsafe_allow_html=True)

# Additional data loading functions for download functionality
def carregar_information_sources():
    """Load information sources using existing backend"""
    df = load_eswd_data()
    source_cols = ["id", "source", "url", "title"]
    available_cols = [col for col in source_cols if col in df.columns]
    return df[available_cols] if not df.empty and available_cols else pd.DataFrame()

def carregar_spatial_ref_sys():
    """Load spatial reference system data using existing backend"""
    # This would typically come from a separate table
    # For now, return empty dataframe since it's not available in current backend
    return pd.DataFrame()

# Generate Excel download
try:
    df_info_sources = carregar_information_sources()
    df_spatial_ref = carregar_spatial_ref_sys()
    
    # Generate Excel in memory
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        if not df_disasters_raw.empty:
            df_disasters_raw.to_excel(writer, sheet_name="disasters", index=False)
        if not df_human_impacts.empty:
            df_human_impacts.to_excel(writer, sheet_name="human_impacts", index=False)
        if not df_loc_disasters.empty:
            df_loc_disasters.to_excel(writer, sheet_name="location", index=False)
        if not df_scraper.empty:
            df_scraper.to_excel(writer, sheet_name="google_scraper", index=False)
        if not df_info_sources.empty:
            df_info_sources.to_excel(writer, sheet_name="information_sources", index=False)
        if not df_spatial_ref.empty:
            df_spatial_ref.to_excel(writer, sheet_name="spatial_ref_sys", index=False)
    
    excel_buffer.seek(0)
    
    # Centered download button
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
    
except Exception as e:
    st.warning(f"Erro ao gerar ficheiro Excel: {str(e)}")

# Refresh button
if st.sidebar.button("Atualizar Dados"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()
