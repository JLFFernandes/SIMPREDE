import pandas as pd
import json  # Importar para carregar o JSON
import re
import joblib
from datetime import datetime
from joblib import load

# Caminho do ficheiro de entrada
input_path = "data/artigos_google_municipios_pt.csv"
output_path = "data/artigos_publico.csv"
municipios_path = "config/municipios_por_distrito.json"  # Caminho para o JSON

# Carregar distritos e paróquias válidas do JSON
with open(municipios_path, 'r', encoding='utf-8') as file:
    municipios_data = json.load(file)

# Extrair distritos válidos
distritos_validos = set(municipios_data.keys())

# Extrair paróquias válidas
paroquias_validas = {par for municipios in municipios_data.values() for par in municipios}

# Indicadores de artigos internacionais
palavras_excluidas = [
    "espanha", "franca", "nepal", "cuba", "eua", "brasil", "japao", "valencia",
    "internacional", "mundo", "europa", "america", "africa", "global", "china"
]

def filtra_artigo_nacional(row):
    url = str(row['page']).lower()
    distrito = str(row['district']).strip().title()
    par = str(row['parish']).strip().title()

    # Verificar se o distrito é válido
    if not distrito or distrito not in distritos_validos:
        return False

    # Verificar se a paróquia é válida
    if par and par not in paroquias_validas:
        return False

    # Verificar palavras excluídas na URL
    if any(palavra in url for palavra in palavras_excluidas):
        return False

    return True

# Carregar CSV
df = pd.read_csv(input_path)

# Aplicar filtro de vítimas
df_vitimas = df[
    (df['fatalities'] >= 0) |
    (df['injured'] >= 0) |
    (df['evacuated'] >= 0) |
    (df['displaced'] >= 0) |
    (df['missing'] >= 0)
]



# Aplicar filtro nacional
df_vitimas = df_vitimas[df_vitimas.apply(filtra_artigo_nacional, axis=1)]



# Palavras e domínios que indicam possível irrelevância
palavras_indesejadas = [
    "brasil", "espanh", "venezuela", "cuba", "nepal", "china", "argentina", "eua", "angola",
    "moçambique", "india", "internacional", "global", "historia", "histórico", "históricas",
    "retrospectiva", "em.com.br", "correiobraziliense", "aviso-amarelo", "aviso-laranja", "previsao", "g1.globo.com", "alerta", "previsto", "emite aviso", "incendios", 
    "desporto", "preve", "avisos", "alertas", "alerta", "aviso", "previsão", "previsões", "previsões meteorológicas", ".com.br", "moçambique", "futuro", "nationalgeographic", "colisao", "belgica"
]

# Ano atual
ano_atual = datetime.now().year

# Filtro baseado no conteúdo da URL
df_vitimas = df_vitimas[~df_vitimas["page"].str.contains("|".join(palavras_indesejadas), case=False, na=False)]

# Filtro de datas antigas e futuras irreais
df_vitimas = df_vitimas[df_vitimas["year"].between(2017, ano_atual)]



vectorizer = load("models/tfidf_vectorizer.pkl")
model = load("models/modelo_classificacao.pkl")

# Aplicar classificador aos títulos
titulos = df_vitimas["page"].astype(str)
X_tfidf = vectorizer.transform(titulos)
predicoes = model.predict(X_tfidf)

# Manter apenas os artigos classificados como relevantes
df_vitimas = df_vitimas[predicoes == 1]

# Eliminar duplicados
df_vitimas.drop_duplicates(subset='page', inplace=True)

# Remover linhas em branco
df_vitimas.dropna(how='all', inplace=True)



# Caminho para o ficheiro JSON de eventos climáticos
eventos_path = "config/eventos_climaticos.json"

# Carregar eventos climáticos a partir do ficheiro JSON
with open(eventos_path, 'r', encoding='utf-8') as file:
    eventos_climaticos = json.load(file)

def identificar_evento(url):
    if isinstance(url, str):
        for e in eventos_climaticos:
            if e in url.lower():
                return e
    return None

df_vitimas["evento_nome"] = df_vitimas["page"].apply(identificar_evento)

# Preencher eventos não identificados
df_vitimas["evento_nome"].fillna("desconhecido", inplace=True)

# Remover duplicados com base em evento climático, data e impacto
df_vitimas = df_vitimas.drop_duplicates(
    subset=["evento_nome", "date", "fatalities", "injured", "displaced"],
    keep="first"
)

 # Guardar todos os artigos num CSV principal
df_vitimas.to_csv("artigos_filtrados.csv", index=False)

# Guardar artigos separados por fonte
fontes = ["jn", "publico", "cnnportugal", "sicnoticias"]

for fonte in fontes:
    df_fonte = df_vitimas[df_vitimas["page"].str.contains(fonte, case=False, na=False)]
    if not df_fonte.empty:
        df_fonte.to_csv(f"artigos_{fonte}.csv", index=False)

print(f"Ficheiro guardado em: {output_path}")
