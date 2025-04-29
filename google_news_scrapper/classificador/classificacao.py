# classificador/classificacao_heuristica.py

import pandas as pd
import re
import os

# === Caminhos ===
INPUT_CSV = "data/intermediate_google_news.csv"
OUTPUT_CSV = "data/intermediate_google_news_labeled_refined.csv"

# Check if the input file exists
if not os.path.exists(INPUT_CSV):
    raise FileNotFoundError(f"❌ Erro: O arquivo de entrada '{INPUT_CSV}' não foi encontrado. Certifique-se de que o 'run_scraper.py' foi executado corretamente.")

# === Heurística Refinada ===
def is_relevant(title):
    title = str(title).lower().strip()

    # 1️⃣ Padrões irrelevantes (exclusão direta)
    padroes_irrelevantes = [
        r"chuva de gol", r"chuva de golos", r"goleada",  # desporto
        r"oferta", r"promo(ç|c)[aã]o", r"desconto", r"loja",  # marketing
        r"campanha", r"publicidade", r"evento cultural", r"exposição",
        r"aeroporto", r"bagagens", r"mercado", r"cheia de alegria", r"repleto de", r"cheia de", r"cheio de",
        r"aviso de .*", r"alerta de .*", r"sob aviso",  # avisos genéricos
        r"cultura", r"música", r"festival", r"gastronomia", r"turismo",
        r"greve", r"protesto", r"incêndio", r"fogo", r"acidente.*rodoviário", r"IPMA"
    ]
    if any(re.search(pat, title) for pat in padroes_irrelevantes):
        return 0

    # 2️⃣ Palavras-chave relevantes (eventos geofísicos com impacto humano)
    keywords_relevantes = [
        "cheia", "cheias", "inundação", "inundações",
        "desabamento", "derrocada", "deslizamento", "alagamento",
        "transbordo", "queda de terra", "mau tempo", "temporal", "chuva forte", "precipitação",
        "tempestade", "vendaval", "vento forte", "ciclone", "tornado",
        "vítimas", "feridos", "mortos", "evacuados", "desaparecidos", "deslocados",
        "estragos", "danos", "habitações", "casas danificadas", "vias cortadas", "estradas cortadas"
    ]
    if any(kw in title for kw in keywords_relevantes):
        return 1

    return 0  # caso contrário, não relevante


# === Aplicar classificação ===
df = pd.read_csv(INPUT_CSV)
df["label"] = df["title"].apply(is_relevant)

# ✅ Filtrar apenas artigos relevantes
df_relevantes = df[df["label"] == 1].copy()
df_irrelevantes = df[df["label"] == 0].copy()

# ✅ Garantir que ambas as classes estão presentes
if df_relevantes.empty or df_irrelevantes.empty:
    raise ValueError("❌ Erro: O dataset filtrado contém apenas uma classe. Ajuste os critérios de filtragem.")

# ✅ Balancear as classes (opcional: limitar o número de irrelevantes para balancear)
df_irrelevantes = df_irrelevantes.sample(n=len(df_relevantes), random_state=42) if len(df_irrelevantes) > len(df_relevantes) else df_irrelevantes

# ✅ Combinar as classes
df = pd.concat([df_relevantes, df_irrelevantes])

# Guardar CSV anotado
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)  # Criar diretório se não existir
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

# Estatísticas
num_relevantes = len(df_relevantes)
num_irrelevantes = len(df_irrelevantes)
total = len(pd.read_csv(INPUT_CSV))  # total original
print(f"✅ CSV com rótulos salvo em: {OUTPUT_CSV}")
print(f"📊 Notícias relevantes: {num_relevantes}, irrelevantes: {num_irrelevantes}, total: {total}")
print(f"📊 Proporção de relevantes: {(num_relevantes/total)*100:.2f}%")
