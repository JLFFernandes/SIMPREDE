import pandas as pd
import json  # Importar para carregar o JSON
import re
import joblib

# Caminho do ficheiro de entrada
input_path = "data/artigos_google_municipios_pt.csv"
output_path = "artigos_com_vitimas.csv"
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
    (df['fatalities'] > 0) |
    (df['injured'] > 0) |
    (df['evacuated'] > 0) |
    (df['displaced'] > 0) |
    (df['missing'] > 0)
]


# Aplicar filtro nacional
df_vitimas = df_vitimas[df_vitimas.apply(filtra_artigo_nacional, axis=1)]

# Carregar modelos treinados
from joblib import load

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

# Guardar novo ficheiro
df_vitimas.to_csv(output_path, index=False)

print(f"Ficheiro guardado em: {output_path}")
