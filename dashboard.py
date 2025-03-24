import sqlite3
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output
import pandas as pd
import dash_daq as daq
import plotly.graph_objs as go
import datetime

# Criar o app Dash com tema escuro Cyborg
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SLATE])

# Função para carregar os dados do banco
def carregar_dados():
    try:
        conn = sqlite3.connect("dados_ocorrencias.db")
        query = """
            SELECT 
                COUNT(CASE WHEN estado = 'Ativa' THEN 1 END) AS total_ativas,
                COUNT(*) AS total_ocorrencias,
                SUM(operacionais) AS total_operacionais,
                SUM(meios_terrestres) AS total_terrestres,
                SUM(meios_aereos) AS total_aereos
            FROM ocorrencias
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df.iloc[0]
    except Exception as e:
        print(f"Erro ao carregar dados do banco: {e}")
        return [0] * 5

# Função para carregar ocorrências ativas por distrito
def carregar_ocorrencias_por_distrito():
    try:
        conn = sqlite3.connect("dados_ocorrencias.db")
        query = """
            SELECT distrito, COUNT(*) AS total
            FROM ocorrencias
            WHERE estado = 'Ativa'
            GROUP BY distrito
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Erro ao carregar dados de ocorrências por distrito: {e}")
        return pd.DataFrame(columns=["distrito", "total"])

# Função para carregar ocorrências totais por distrito no presente dia
def carregar_ocorrencias_dia_por_distrito():
    try:
        conn = sqlite3.connect("dados_ocorrencias.db")
        data_hoje = datetime.date.today().strftime("%Y-%m-%d")
        query = f"""
            SELECT distrito, COUNT(*) AS total
            FROM ocorrencias
            WHERE data = '{data_hoje}'
            GROUP BY distrito
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Erro ao carregar dados de ocorrências do dia por distrito: {e}")
        return pd.DataFrame(columns=["distrito", "total"])

# Layout modernizado baseado no modelo Manufacturing SPC Dashboard
app.layout = dbc.Container(fluid=True, children=[
    dbc.Row([
        dbc.Col(html.H1("Painel de Ocorrências", className="text-center text-light my-4"), width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            daq.LEDDisplay(id='total-ativas', color="#FF4500", size=60),
            html.H4("Ocorrências Ativas", className="text-center")
        ], width=3),
        dbc.Col([
            daq.LEDDisplay(id='total-ocorrencias', color="#FFA500", size=60),
            html.H4("Total de Ocorrências", className="text-center")
        ], width=3),
        dbc.Col([
            daq.LEDDisplay(id='total-operacionais', color="#00FF00", size=60),
            html.H4("Operacionais", className="text-center")
        ], width=3),
        dbc.Col([
            daq.LEDDisplay(id='total-meios-terrestres', color="#00BFFF", size=60),
            html.H4("Meios Terrestres", className="text-center")
        ], width=3)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col(dcc.Graph(id='grafico-ocorrencias-distrito'), width=6),
        dbc.Col(dcc.Graph(id='grafico-ocorrencias-dia-distrito'), width=6)
    ], className="mb-4"),

    dcc.Interval(id='interval-component', interval=10*1000, n_intervals=0)
])

# Callback para atualização automática
@app.callback(
    [
        Output("total-ativas", "value"),
        Output("total-ocorrencias", "value"),
        Output("total-operacionais", "value"),
        Output("total-meios-terrestres", "value"),
        Output("grafico-ocorrencias-distrito", "figure"),
        Output("grafico-ocorrencias-dia-distrito", "figure")
    ],
    Input("interval-component", "n_intervals")
)
def atualizar_ocorrencias(n):
    dados = carregar_dados()
    df_ocorrencias = carregar_ocorrencias_por_distrito()
    df_ocorrencias_dia = carregar_ocorrencias_dia_por_distrito()
    
    fig_distrito = go.Figure([go.Bar(x=df_ocorrencias["distrito"], y=df_ocorrencias["total"], marker_color="#FF4500")])
    fig_distrito.update_layout(title="Ocorrências Ativas por Distrito", plot_bgcolor="#121212", paper_bgcolor="#121212", font=dict(color="white"))
    
    fig_dia_distrito = go.Figure([go.Bar(x=df_ocorrencias_dia["distrito"], y=df_ocorrencias_dia["total"], marker_color="#00BFFF")])
    fig_dia_distrito.update_layout(title="Ocorrências Totais por Distrito Hoje", plot_bgcolor="#121212", paper_bgcolor="#121212", font=dict(color="white"))
    
    return (
        dados["total_ativas"],
        dados["total_ocorrencias"],
        dados["total_operacionais"],
        dados["total_terrestres"],
        fig_distrito,
        fig_dia_distrito
    )

# Rodar o servidor do dashboard
if __name__ == '__main__':
    app.run_server(debug=False)
