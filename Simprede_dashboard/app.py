import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from utils.supabase_connector import SupabaseConnection

# Page configuration
st.set_page_config(
    page_title="SIMPREDE - Sistema de Prevenção de Desastres",
    page_icon="🚨",
    layout="wide"
)

# Initialize connection
@st.cache_resource
def init_connection():
    return SupabaseConnection()

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

# Helper function to safely get unique values excluding None
def get_unique_values(series, exclude_none=True):
    """Get unique values from a pandas Series, optionally excluding None values"""
    if exclude_none:
        return sorted([x for x in series.unique() if x is not None and pd.notna(x)])
    else:
        return sorted(series.unique())

# Header with centered logo and title
# Center the logo at the top middle of the page
col1, col_logo, col3 = st.columns([1, 2, 1])
with col_logo:
    st.image("logo.png", width=3200)

# Title below the logo, centered
st.markdown("<h1 style='color: #1f77b4; margin-bottom: 0; text-align: center;'>SIMPREDE - Sistema de Prevenção de Desastres</h1>", unsafe_allow_html=True)


# Database selection using radio buttons (section menu)
st.sidebar.title("Selecionar Base de Dados")

# Create radio button options for database selection
database_options = [
    "Ocorrências Históricas de Desastres", 
    "Google Scraper - Daily Scrapping Google News"
]

selected_database = st.sidebar.radio(
    "Escolha a fonte de dados:",
    database_options,
    index=0,
    format_func=lambda x: f"{x}"  # Remove bullet point styling
)

# Extract the actual page name from the selection
page = selected_database

if page == "Google Scraper - Daily Scrapping Google News":
    # Main app
    st.markdown("**Dashboard de Monitorização de Ocorrências de Emergência**")

    # Load data
    with st.spinner("A carregar dados das ocorrências..."):
        df_ocorrencias = load_ocorrencias_data()
        df_locations = load_location_data()

    if df_ocorrencias.empty:
        st.warning("Nenhuma ocorrência encontrada (excluindo categoria 'Other')")
        st.stop()

    # Sidebar filters
    st.sidebar.header("Filtros")

    # Year filter with "All Years" as default
    if 'year' in df_ocorrencias.columns:
        anos_disponiveis = ['Todos os Anos'] + get_unique_values(df_ocorrencias['year'])
        ano_selecionado = st.sidebar.selectbox("Ano", anos_disponiveis, index=0)
        
        if ano_selecionado == 'Todos os Anos':
            df_filtered = df_ocorrencias
        else:
            df_filtered = df_ocorrencias[df_ocorrencias['year'] == ano_selecionado]
    else:
        df_filtered = df_ocorrencias
        ano_selecionado = 'Todos os Anos'

    # Type filter
    if 'type' in df_filtered.columns:
        tipos_unicos = [t for t in df_filtered['type'].unique() if t != 'Other' and t is not None and pd.notna(t)]
        tipos_disponiveis = ['Todos'] + sorted(tipos_unicos)
        tipo_selecionado = st.sidebar.selectbox("Tipo de Ocorrência", tipos_disponiveis)
        if tipo_selecionado != 'Todos':
            df_filtered = df_filtered[df_filtered['type'] == tipo_selecionado]

    # District filter
    if 'district' in df_filtered.columns:
        distritos_disponiveis = ['Todos'] + get_unique_values(df_filtered['district'])
        distrito_selecionado = st.sidebar.selectbox("Distrito", distritos_disponiveis)
        if distrito_selecionado != 'Todos':
            df_filtered = df_filtered[df_filtered['district'] == distrito_selecionado]

    # Main metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total de Ocorrências", len(df_filtered))

    with col2:
        total_fatalities = df_filtered['fatalities'].sum() if 'fatalities' in df_filtered.columns else 0
        st.metric("Fatalidades", int(total_fatalities))

    with col3:
        total_injured = df_filtered['injured'].sum() if 'injured' in df_filtered.columns else 0
        st.metric("Feridos", int(total_injured))

    with col4:
        unique_districts = df_filtered['district'].nunique() if 'district' in df_filtered.columns else 0
        st.metric("Distritos Afetados", unique_districts)

    # Map section
    st.subheader("Localização das Ocorrências")

    if not df_locations.empty and 'latitude' in df_locations.columns:
        df_map_filtered = df_locations.copy()
        
        if ano_selecionado != 'Todos os Anos' and 'year' in df_map_filtered.columns:
            df_map_filtered = df_map_filtered[df_map_filtered['year'] == ano_selecionado]
        
        if tipo_selecionado != 'Todos' and 'type' in df_map_filtered.columns:
            df_map_filtered = df_map_filtered[df_map_filtered['type'] == tipo_selecionado]
            
        if distrito_selecionado != 'Todos' and 'district' in df_map_filtered.columns:
            df_map_filtered = df_map_filtered[df_map_filtered['district'] == distrito_selecionado]
        
        if not df_map_filtered.empty:
            fig = px.scatter_map(
                df_map_filtered,
                lat="latitude",
                lon="longitude",
                color="type",
                hover_data=["district", "municipality", "fatalities", "injured"],
                zoom=5.5,  # Reduced zoom to show all of Portugal
                height=600,  # Increased height for better visibility
                title=f"Distribuição Geográfica das Ocorrências - {ano_selecionado}",
                center={"lat": 39.5, "lon": -8.0}  # Center on Portugal mainland
            )
            fig.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0,"t":50,"l":0,"b":0}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Nenhuma ocorrência encontrada para os filtros selecionados")

    # Charts section
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Ocorrências por Tipo")
        if 'type' in df_filtered.columns:
            type_counts = df_filtered['type'].value_counts()
            if not type_counts.empty:
                fig_pie = px.pie(values=type_counts.values, names=type_counts.index,
                               title=f"Distribuição por Tipo - {ano_selecionado}")
                st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        st.subheader("Ocorrências por Distrito")
        if 'district' in df_filtered.columns:
            district_counts = df_filtered['district'].value_counts().head(10)
            if not district_counts.empty:
                fig_bar = px.bar(x=district_counts.values, y=district_counts.index, orientation='h',
                               title="Top 10 Distritos")
                fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_bar, use_container_width=True)

    # Time series chart - WEEKLY evolution for Google Scraper (always shown)
    st.subheader("Evolução Temporal das Ocorrências")
    if 'date' in df_filtered.columns:
        df_temp = df_filtered.copy()
        if not pd.api.types.is_datetime64_any_dtype(df_temp['date']):
            df_temp['date'] = pd.to_datetime(df_temp['date'], dayfirst=True, errors='coerce')
        
        df_temp = df_temp.dropna(subset=['date'])
        
        if not df_temp.empty:
            df_temp['week'] = df_temp['date'].dt.to_period('W').dt.start_time
            weekly_counts = df_temp.groupby('week').size().reset_index(name='count')
            
            fig_timeline = px.line(weekly_counts, 
                                 x='week', 
                                 y='count',
                                 title=f"Evolução Semanal das Ocorrências - {ano_selecionado}",
                                 labels={'week': 'Semana', 'count': 'Número de Ocorrências'})
            
            fig_timeline.update_layout(
                xaxis_title="Semana",
                yaxis_title="Número de Ocorrências",
                hovermode='x unified'
            )
            fig_timeline.update_traces(
                hovertemplate='<b>Semana:</b> %{x}<br><b>Ocorrências:</b> %{y}<extra></extra>'
            )
            
            st.plotly_chart(fig_timeline, use_container_width=True)
        else:
            st.warning("Não há dados de data válidos para mostrar a evolução temporal")
    else:
        st.warning("Coluna 'date' não encontrada nos dados")

    # Recent events table
    st.subheader("Ocorrências Recentes")
    if 'created_at' in df_filtered.columns:
        recent_events = df_filtered.sort_values('created_at', ascending=False).head(10)
        display_cols = ['date', 'type', 'subtype', 'district', 'municipality', 'fatalities', 'injured']
        available_cols = [col for col in display_cols if col in recent_events.columns]
        st.dataframe(recent_events[available_cols], use_container_width=True)

elif page == "Ocorrências Históricas de Desastres":
    # Historical Disasters Section
    st.markdown("**Base de Dados de Ocorrências Históricas de Desastres**")

    # Load ESWD data - now automatically handles pagination to get ALL records
    with st.spinner("A carregar TODOS os dados históricos de desastres (isto pode demorar um pouco para 2310+ registos)..."):
        df_eswd = load_eswd_data()

    if df_eswd.empty:
        st.warning("Nenhum dado histórico de desastres encontrado")
        st.info("Certifique-se de que as tabelas 'disasters', 'location', 'human_impacts' e 'information_sources' existem na base de dados.")
        st.stop()

    # Historical Disasters Filters
    st.sidebar.header("Filtros de Desastres Históricos")

    # Year filter for Historical Disasters with "All Years" as default
    if 'year' in df_eswd.columns:
        anos_eswd = ['Todos os Anos'] + get_unique_values(df_eswd['year'])
        ano_eswd = st.sidebar.selectbox("Ano", anos_eswd, index=0, key="disaster_year")
        
        if ano_eswd == 'Todos os Anos':
            df_eswd_filtered = df_eswd
        else:
            df_eswd_filtered = df_eswd[df_eswd['year'] == ano_eswd]
    else:
        df_eswd_filtered = df_eswd
        ano_eswd = 'Todos os Anos'

    # Type filter for Historical Disasters
    if 'type' in df_eswd_filtered.columns:
        tipos_eswd = ['Todos'] + get_unique_values(df_eswd_filtered['type'])
        tipo_eswd = st.sidebar.selectbox("Tipo de Desastre", tipos_eswd, key="disaster_type")
        if tipo_eswd != 'Todos':
            df_eswd_filtered = df_eswd_filtered[df_eswd_filtered['type'] == tipo_eswd]

    # District filter for Historical Disasters
    if 'district' in df_eswd_filtered.columns:
        distritos_eswd = ['Todos'] + get_unique_values(df_eswd_filtered['district'])
        distrito_eswd = st.sidebar.selectbox("Distrito", distritos_eswd, key="disaster_district")
        if distrito_eswd != 'Todos':
            df_eswd_filtered = df_eswd_filtered[df_eswd_filtered['district'] == distrito_eswd]

    # Historical Disasters Metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Desastres Históricos", len(df_eswd_filtered))

    with col2:
        total_fatalities_eswd = df_eswd_filtered['fatalities'].sum() if 'fatalities' in df_eswd_filtered.columns else 0
        st.metric("Fatalidades", int(total_fatalities_eswd))

    with col3:
        total_injured_eswd = df_eswd_filtered['injured'].sum() if 'injured' in df_eswd_filtered.columns else 0
        st.metric("Feridos", int(total_injured_eswd))

    with col4:
        total_evacuated = df_eswd_filtered['evacuated'].sum() if 'evacuated' in df_eswd_filtered.columns else 0
        st.metric("Evacuados", int(total_evacuated))

    # Historical Disasters Map
    st.subheader("Localização dos Desastres Históricos")
    if 'latitude' in df_eswd_filtered.columns and 'longitude' in df_eswd_filtered.columns:
        df_eswd_map = df_eswd_filtered.dropna(subset=['latitude', 'longitude'])
        df_eswd_map = df_eswd_map[
            (df_eswd_map['latitude'].between(-90, 90)) & 
            (df_eswd_map['longitude'].between(-180, 180))
        ]
        
        if not df_eswd_map.empty:
            fig_eswd = px.scatter_map(
                df_eswd_map,
                lat="latitude",
                lon="longitude",
                color="type",
                hover_data=["district", "municipality", "fatalities", "injured"],
                zoom=5.5,  # Reduced zoom to show all of Portugal
                height=600,  # Increased height for better visibility
                title=f"Distribuição Geográfica dos Desastres Históricos - {ano_eswd}",
                center={"lat": 39.5, "lon": -8.0}  # Center on Portugal mainland
            )
            fig_eswd.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0,"t":50,"l":0,"b":0}
            )
            st.plotly_chart(fig_eswd, use_container_width=True)

    # Historical Disasters Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Desastres Históricos por Tipo")
        if 'type' in df_eswd_filtered.columns:
            type_counts_eswd = df_eswd_filtered['type'].value_counts()
            if not type_counts_eswd.empty:
                fig_pie_eswd = px.pie(values=type_counts_eswd.values, names=type_counts_eswd.index,
                                    title=f"Distribuição por Tipo de Desastre - {ano_eswd}")
                st.plotly_chart(fig_pie_eswd, use_container_width=True)

    with col2:
        st.subheader("Desastres Históricos por Distrito")
        if 'district' in df_eswd_filtered.columns:
            district_counts_eswd = df_eswd_filtered['district'].value_counts().head(10)
            if not district_counts_eswd.empty:
                fig_bar_eswd = px.bar(x=district_counts_eswd.values, y=district_counts_eswd.index, 
                                    orientation='h', title="Top 10 Distritos - Desastres Históricos")
                fig_bar_eswd.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_bar_eswd, use_container_width=True)

    # Time series chart for Historical Disasters - YEARLY evolution (only when "All Years" is selected)
    if ano_eswd == 'Todos os Anos' and 'year' in df_eswd_filtered.columns:
        st.subheader("Evolução Temporal dos Desastres Históricos")
        yearly_counts_eswd = df_eswd_filtered['year'].value_counts().sort_index()
        fig_timeline_eswd = px.line(x=yearly_counts_eswd.index, y=yearly_counts_eswd.values,
                                   title="Número de Desastres Históricos por Ano",
                                   labels={'x': 'Ano', 'y': 'Número de Desastres'})
        
        fig_timeline_eswd.update_layout(
            xaxis_title="Ano",
            yaxis_title="Número de Desastres",
            hovermode='x unified'
        )
        fig_timeline_eswd.update_traces(
            hovertemplate='<b>Ano:</b> %{x}<br><b>Desastres:</b> %{y}<extra></extra>'
        )
        
        st.plotly_chart(fig_timeline_eswd, use_container_width=True)

    # Historical Disasters Data Table with pagination
    st.subheader("Dados Históricos de Desastres Detalhados")
    
    # Pagination controls
    total_records = len(df_eswd_filtered)
    
    if total_records > 0:
        # Pagination settings
        records_per_page = st.selectbox(
            "Registos por página:", 
            [25, 50, 100, 500, 1000], 
            index=1,  # Default to 50
            key="disaster_records_per_page"
        )
        
        total_pages = (total_records - 1) // records_per_page + 1
        
        # Page navigation
        col_page1, col_page2, col_page3 = st.columns([2, 1, 2])
        
        with col_page1:
            page_number = st.number_input(
                "Página:", 
                min_value=1, 
                max_value=total_pages, 
                value=1,
                key="disaster_page_number"
            )
        
        with col_page2:
            st.metric("Total Páginas", total_pages)
        
        with col_page3:
            st.metric("Total Registos", f"{total_records:,}")
        
        # Calculate start and end indices
        start_idx = (page_number - 1) * records_per_page
        end_idx = min(start_idx + records_per_page, total_records)
        
        # Show current page info
        st.info(f"Página {page_number} de {total_pages} | Mostrando registos {start_idx + 1} a {end_idx} de {total_records:,}")
        
        # Display the paginated data
        display_cols_eswd = ['date', 'type', 'subtype', 'district', 'municipality', 'fatalities', 'injured', 'evacuated']
        available_cols_eswd = [col for col in display_cols_eswd if col in df_eswd_filtered.columns]
        
        paginated_data = df_eswd_filtered[available_cols_eswd].iloc[start_idx:end_idx]
        st.dataframe(paginated_data, use_container_width=True)
        
        # Navigation buttons
        col_nav1, col_nav2, col_nav3, col_nav4 = st.columns(4)
        
        with col_nav1:
            if st.button("Primeira", disabled=(page_number == 1), key="disaster_first_page"):
                st.session_state.disaster_page_number = 1
                st.rerun()
        
        with col_nav2:
            if st.button("Anterior", disabled=(page_number == 1), key="disaster_prev_page"):
                st.session_state.disaster_page_number = max(1, page_number - 1)
                st.rerun()
        
        with col_nav3:
            if st.button("Próxima", disabled=(page_number == total_pages), key="disaster_next_page"):
                st.session_state.disaster_page_number = min(total_pages, page_number + 1)
                st.rerun()
        
        with col_nav4:
            if st.button("Última", disabled=(page_number == total_pages), key="disaster_last_page"):
                st.session_state.disaster_page_number = total_pages
                st.rerun()
    else:
        st.warning("Nenhum registro encontrado com os filtros aplicados.")

# Refresh button
if st.sidebar.button("Atualizar Dados"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()
