import csv
import sys
from bs4 import BeautifulSoup

# Add the project directory to sys.path
sys.path.append('/home/odiurdigital/Universidade/projeto/simprede/scrapers/news_scraper')

from news_scraper_mz.utils.helpers import (
    fetch_and_extract_article_text,
    extract_victim_counts,
    detect_disaster_type,
    extract_title_from_text,
    verificar_localizacao,
    extract_event_hour,
    limpar_texto_lixo
)

def extract_links_from_csv(csv_path: str) -> list[str]:
    """
    Extracts article links from a CSV file.
    """
    links = []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                print(f"🔍 Row: {row}")  # DEBUG: Print each row
                if "source" in row:  # Adjust column name if necessary
                    # Extract the link from the source column up to the first comma
                    link = row["source"].split(",")[0].strip()
                    print(f"🔗 Extracted link: {link}")  # DEBUG: Print extracted link
                    links.append(link)
    except FileNotFoundError:
        print(f"⚠️ File not found: {csv_path}")  # DEBUG: File not found
    except Exception as e:
        print(f"⚠️ Error reading CSV: {e}")  # DEBUG: General error
    return links

def process_links_to_csv(input_csv: str, output_csv: str):
    """
    Processes links from the input CSV and saves the extracted data to a single CSV file every 10 articles.
    """
    links = extract_links_from_csv(input_csv)
    print(f"🔗 Found {len(links)} links.")  # DEBUG: Print number of links found

    articles = []

    for i, link in enumerate(links, start=1):
        # Validate the link
        if not link.startswith("http"):
            print(f"⚠️ Invalid link format: {link}")  # DEBUG: Invalid link format
            continue

        print(f"Processing link: {link}")  # DEBUG: Processing link
        try:
            # Fetch and clean the article text
            article_text_raw = fetch_and_extract_article_text(link)
            article_text = limpar_texto_lixo(article_text_raw)

            if not article_text:
                print(f"⚠️ Texto vazio para o link: {link}")
                continue

            # Extract relevant information
            victim_counts = extract_victim_counts(article_text)
            disaster_type, subtype = detect_disaster_type(article_text)
            title = extract_title_from_text(article_text)
            location = verificar_localizacao(article_text)
            hour = extract_event_hour(article_text)

            # Prepare the article data
            article_data = {
                "ID": link,
                "title": title,
                "date": "",  # Deves extrair ou definir manualmente se possível
                "type": disaster_type,
                "subtype": subtype,
                "district": location.get("district") if location else "",
                "municipali": location.get("municipali") if location else "",
                "parish": location.get("parish") if location else "",
                "dicofreg": location.get("dicofreg") if location else "",
                "hour": hour if hour else "",
                "source": link,
                "link_extraido": link,
                "fatalities": victim_counts.get("fatalities", 0),
                "injured": victim_counts.get("injured", 0),
                "evacuated": victim_counts.get("evacuated", 0),
                "displaced": victim_counts.get("displaced", 0),
                "missing": victim_counts.get("missing", 0),
                "sourcetype": "scraped",
            }
            articles.append(article_data)

            # Save every 10 articles
            if len(articles) == 10:
                print(f"💾 Saving batch of 10 articles...")  # DEBUG: Saving batch
                save_articles_to_csv(output_csv, articles, append=True)
                articles = []  # Clear the batch

        except Exception as e:
            print(f"⚠️ Error processing link {link}: {e}")  # DEBUG: Error processing link

    # Save any remaining articles
    if articles:
        print(f"💾 Saving remaining {len(articles)} articles...")  # DEBUG: Saving remaining articles
        save_articles_to_csv(output_csv, articles, append=True)

def save_articles_to_csv(output_csv: str, articles: list[dict], append: bool = False):
    """
    Saves a list of articles to a CSV file.
    """
    mode = "a" if append else "w"
    with open(output_csv, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=articles[0].keys())
        if not append:  # Write header only if not appending
            writer.writeheader()
        writer.writerows(articles)
    print(f"✅ Data saved to {output_csv}")  # DEBUG: Data saved

# Example usage
if __name__ == "__main__":
    input_csv = "data/artigos_google_municipios_mz.csv"
    output_csv = "disaster_db_ready.csv"
    process_links_to_csv(input_csv, output_csv)
