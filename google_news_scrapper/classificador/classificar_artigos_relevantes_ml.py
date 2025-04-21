#Classificação de artigos relevantes com Machine Learning

import pandas as pd
import joblib
import os

# Caminhos dos ficheiros
INPUT_CSV = "data/intermediate_google_news.csv"
VECTORIZER_PATH = "models/tfidf_vectorizer.pkl"
MODEL_PATH = "models/modelo_classificacao.pkl"
OUTPUT_CSV = "data/intermediate_google_news_relevantes.csv"

# 1️⃣ Carregar o CSV com os artigos intermédios
df = pd.read_csv(INPUT_CSV)

if "title" not in df.columns:
    raise ValueError("❌ O ficheiro CSV não contém a coluna 'title'.")

print(f"📄 Total de artigos carregados: {len(df)}")

# 2️⃣ Carregar o vectorizador e modelo treinado
vectorizer = joblib.load(VECTORIZER_PATH)
model = joblib.load(MODEL_PATH)

# 3️⃣ Transformar os títulos
X = vectorizer.transform(df["title"].astype(str))

# 4️⃣ Fazer previsões
preds = model.predict(X)
df["label"] = preds

# 5️⃣ Filtrar apenas os artigos relevantes (label = 1)
df_relevantes = df[df["label"] == 1].copy()
print(f"✅ Artigos relevantes encontrados: {len(df_relevantes)}")

# 6️⃣ Guardar num novo CSV
df_relevantes.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
print(f"💾 Ficheiro guardado em: {OUTPUT_CSV}")
