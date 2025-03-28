from scrapers import arquivo_pt
from utils.helpers import guardar_csv

# 🔧 Definir os domínios dos jornais
PUBLICO = "www.publico.pt"
JN = "www.jn.pt"


def run_all():
    print("\n🚀 Iniciando scraping de fontes de notícias...\n")
    all_articles = []

    # Se quiseres voltar a ativar o scraping do Arquivo.pt:
    for jornal in [PUBLICO, JN]:
        print(f"\n📰 A processar artigos de: {jornal}")
        artigos = arquivo_pt.scrape(site=jornal)
        all_articles.extend(artigos)

    # Capturar e juntar artigos da NewsAPI
   # artigos_newsapi = news_api.run_newsapi_scraper()
   # all_articles.extend(artigos_newsapi)

    # Guardar o ficheiro final
    guardar_csv("data/artigos_filtrados.csv", all_articles)
    print(f"\n✅ Scraping completo. Total: {len(all_articles)} artigos guardados.\n")

if __name__ == "__main__":
    run_all()
