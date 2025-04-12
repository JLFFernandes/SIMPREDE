import pandas as pd

# Carregar ficheiro intermédio
df = pd.read_csv("../data/intermediate_google_news.csv")  # Adjusted file path

# Lista de keywords para treino
keywords = ["ocorrencias", "cheias", "inundações", "desabamento", "mau tempo", "desastre", "chuva forte", "tempestade", "vítimas", "desclocados", "evacuados", "desaparecidos", "feridos", "mortos", "deslizamento", "alagamento", "transbordo", "queda de terra"]

# Heurística simples
def is_relevant(title):
    title_lower = str(title).lower()
    return int(any(kw in title_lower for kw in keywords))

# Aplicar classificação
df["label"] = df["title"].apply(is_relevant)

# Guardar novo CSV anotado
df.to_csv("data/intermediate_google_news_labeled.csv", index=True, encoding="utf-8")  # Adjusted file path
print("✅ CSV com rótulos salvo!")


