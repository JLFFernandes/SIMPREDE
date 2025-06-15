import streamlit as st

def display_metrics_cards(df_filtrado):
    """
    Exibe cards de métricas principais de forma modular
    """
    # Calcular métricas
    total_eventos = len(df_filtrado)
    total_vitimas = df_filtrado['Vitimas'].sum()
    total_fontes = df_filtrado['Fonte'].nunique()
    
    # Calcular evento mais frequente
    if not df_filtrado.empty:
        evento_mais_frequente = df_filtrado['Evento'].mode().iloc[0]
    else:
        evento_mais_frequente = "N/A"
    
    # Layout em colunas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🔢 Total de Eventos",
            value=total_eventos,
            help="Número total de eventos registados no período selecionado"
        )
    
    with col2:
        st.metric(
            label="👥 Total de Vítimas",
            value=int(total_vitimas),
            help="Soma total de vítimas em todos os eventos"
        )
    
    with col3:
        st.metric(
            label="📰 Fontes Únicas",
            value=total_fontes,
            help="Número de fontes diferentes que reportaram eventos"
        )
    
    with col4:
        st.metric(
            label="⚡ Evento Mais Frequente",
            value=evento_mais_frequente,
            help="Tipo de evento que ocorreu mais vezes"
        )

def display_regional_metrics(df_filtrado):
    """
    Exibe métricas específicas por região
    """
    if df_filtrado.empty:
        st.info("Nenhum dado disponível para métricas regionais")
        return
    
    st.subheader("📍 Métricas por Região")
    
    # Agrupar por região
    df_regiao = df_filtrado.groupby('Regiao').agg({
        'Vitimas': 'sum',
        'Evento': 'count'
    }).reset_index()
    
    # Renomear colunas
    df_regiao.columns = ['Região', 'Total Vítimas', 'Nº Eventos']
    
    # Exibir em colunas
    num_regioes = len(df_regiao)
    cols = st.columns(min(num_regioes, 5))  # Máximo 5 colunas
    
    for i, (_, row) in enumerate(df_regiao.iterrows()):
        if i < len(cols):
            with cols[i]:
                st.metric(
                    label=f"🏘️ {row['Região']}",
                    value=f"{row['Nº Eventos']} eventos",
                    delta=f"{row['Total Vítimas']} vítimas"
                )

def display_temporal_metrics(df_filtrado):
    """
    Exibe métricas temporais se houver coluna de data
    """
    if 'Data' in df_filtrado.columns:
        st.subheader("⏰ Análise Temporal")
        
        # Converter para datetime se necessário
        df_filtrado['Data'] = pd.to_datetime(df_filtrado['Data'], errors='coerce')
        
        # Métricas temporais
        col1, col2, col3 = st.columns(3)
        
        with col1:
            primeiro_evento = df_filtrado['Data'].min()
            st.metric(
                label="📅 Primeiro Evento",
                value=primeiro_evento.strftime('%d/%m/%Y') if pd.notna(primeiro_evento) else "N/A"
            )
        
        with col2:
            ultimo_evento = df_filtrado['Data'].max()
            st.metric(
                label="📅 Último Evento",
                value=ultimo_evento.strftime('%d/%m/%Y') if pd.notna(ultimo_evento) else "N/A"
            )
        
        with col3:
            if pd.notna(primeiro_evento) and pd.notna(ultimo_evento):
                periodo = (ultimo_evento - primeiro_evento).days
                st.metric(
                    label="📊 Período (dias)",
                    value=periodo
                )

def create_custom_metric_card(title, value, delta=None, delta_color="normal"):
    """
    Cria card de métrica personalizado com estilo
    """
    delta_html = ""
    if delta:
        color = "green" if delta_color == "normal" else "red"
        delta_html = f'<p style="color: {color}; margin: 0; font-size: 0.8rem;">{delta}</p>'
    
    card_html = f"""
    <div style="
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e9ecef;
        text-align: center;
        margin: 0.5rem 0;
    ">
        <h3 style="margin: 0; color: #1f77b4;">{title}</h3>
        <h2 style="margin: 0.5rem 0; color: #333;">{value}</h2>
        {delta_html}
    </div>
    """
    
    st.markdown(card_html, unsafe_allow_html=True)
