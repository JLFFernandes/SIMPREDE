import argparse
from scrapers.google_news import run_scraper as run_google_news
from scrapers.alerta_scraper import run_alerta_scraper

def main():
    parser = argparse.ArgumentParser(description="Scraper de notícias")
    parser.add_argument(
        "--fonte",
        choices=["news", "alert"],
        default="news",
        help="Escolhe a fonte de scraping: 'news' (Google News) ou 'alert' (Google Alerts)",
    )
    parser.add_argument(
        "--alert-url",
        help="URL do feed RSS do Google Alerts (necessário se fonte='alert')",
    )

    args = parser.parse_args()

    if args.fonte == "news":
        print("📰 A correr scraper do Google News...")
        run_google_news()
    elif args.fonte == "alert":
        if not args.alert_url:
            print("❌ Erro: Precisas fornecer o --alert-url para usar o modo 'alert'.")
        else:
            print(f"🔔 A correr scraper do Google Alerts com feed:\n{args.alert_url}")
            run_alerta_scraper(args.alert_url)

if __name__ == "__main__":
    main()
