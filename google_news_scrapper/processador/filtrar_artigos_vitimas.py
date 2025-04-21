import pandas as pd

# Caminho do ficheiro de entrada
input_path = "data/artigos_google_municipios_pt.csv"
output_path = "artigos_com_vitimas.csv"

# Carregar CSV
df = pd.read_csv(input_path)

# Filtrar linhas com pelo menos uma vítima
df_vitimas = df[
    (df['fatalities'] > 0) |
    (df['injured'] > 0) |
    (df['evacuated'] > 0) |
    (df['displaced'] > 0) |
    (df['missing'] > 0)
]

# Guardar novo ficheiro
df_vitimas.to_csv(output_path, index=False)

print(f"Ficheiro guardado em: {output_path}")
