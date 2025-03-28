from scrapers import arquivo_pt
from processors.filtra_desastres import filtrar_e_transformar
from processors.gera_db_ready import gerar_disaster_db_ready

# 🔧 Definir os domínios dos jornais
PUBLICO = "www.publico.pt"
DN = "www.DN.pt"
OUTPUT_CSV = "data/artigos_brutos.csv"


def run_all():
    print("\n🚀 Iniciando scraping de fontes de notícias (Arquivo.pt)...\n")
    all_articles = []

    for jornal in [PUBLICO, DN]:
        artigos = arquivo_pt.scrape(site=jornal)
        all_articles.extend(artigos)

    print(f"\n📎 A guardar {len(all_articles)} artigos em: {OUTPUT_CSV}")
    arquivo_pt.exportar_csv(all_articles, OUTPUT_CSV)

    print("\n🧼 A iniciar filtro e transformação...\n")
    filtrar_e_transformar()

    print("\n📊 A gerar ficheiro final com vítimas...\n")
    gerar_disaster_db_ready()

    print("\n✅ Pipeline completo.\n")


if __name__ == "__main__":
    run_all()