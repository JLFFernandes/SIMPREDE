import pandas as pd

# Caminho para o CSV intermédio com todos os desastres
INPUT_CSV = "data/artigos_filtrados.csv"
OUTPUT_CSV = "data/disaster_db_ready.csv"

# Colunas que indicam vítimas
COLUNAS_VITIMAS = ['fatalities', 'injured', 'evacuated', 'displaced', 'missing']


def gerar_disaster_db_ready():
    print("\n🧪 A processar artigos com vítimas...")

    try:
        df = pd.read_csv(INPUT_CSV)
    except FileNotFoundError:
        print(f"❌ Ficheiro não encontrado: {INPUT_CSV}")
        return

    # Assegura que todos os campos de vítimas são numéricos
    df[COLUNAS_VITIMAS] = df[COLUNAS_VITIMAS].fillna(0)
    df[COLUNAS_VITIMAS] = df[COLUNAS_VITIMAS].apply(pd.to_numeric, errors='coerce').fillna(0)

    # Filtra apenas linhas com pelo menos uma vítima
    df_filtrado = df[df[COLUNAS_VITIMAS].sum(axis=1) > 0]

    if df_filtrado.empty:
        print("⚠️ Nenhum artigo com vítimas encontrado.")
    else:
        df_filtrado.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Exportado: {len(df_filtrado)} artigos com vítimas para {OUTPUT_CSV}")


if __name__ == "__main__":
    gerar_disaster_db_ready()
