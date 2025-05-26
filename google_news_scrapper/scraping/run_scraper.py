import os
import csv
import hashlib
import feedparser
import sys
import time
import random
import asyncio
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential
SEMAPHORE = asyncio.Semaphore(15)
import aiohttp
from datetime import datetime, timedelta
import argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.helpers import load_keywords, carregar_paroquias_com_municipios, gerar_id
from pathlib import Path

# Update template to include date parameters
GOOGLE_NEWS_TEMPLATE = "https://news.google.com/rss/search?q={query}+{municipio}+Portugal&hl=pt-PT&gl=PT&ceid=PT:pt{date_filter}"
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")
LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")
TODAS_LOCALIDADES = list(LOCALIDADES.keys()) + MUNICIPIOS + DISTRITOS
INTERMEDIATE_CSV = "data/raw/intermediate_google_news.csv"

# Rate limiting settings
SEMAPHORE = asyncio.Semaphore(20)  # Increased from 15
CONCURRENT_REQUESTS = 10  # Increased from 5
MIN_DELAY = 0.1  # Decreased from 0.5
MAX_DELAY = 1.0  # Decreased from 2.0
BATCH_SIZE = 30  # Increased from 20

# Add new constants
USER_AGENTS = UserAgent()
CACHE = {}
CACHE_DURATION = 7200  # Increased to 2 hours

def format_date_filter(date_str):
    return ""  # Deixa de confiar no filtro da Google

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
async def fetch_news(session, keyword, localidade, date_filter, dias):
    cache_key = f"{keyword}_{localidade}_{date_filter}_{dias}"
    if cache_key in CACHE:
        cached_time, cached_data = CACHE[cache_key]
        if (time.time() - cached_time) < CACHE_DURATION:
            return cached_data

    async with SEMAPHORE:
        # Randomize delay more naturally
        delay = random.uniform(MIN_DELAY, MAX_DELAY) * (1 + random.random() * 0.5)
        await asyncio.sleep(delay)
        
        headers = {
            "User-Agent": USER_AGENTS.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
        }

        query_url = GOOGLE_NEWS_TEMPLATE.format(
            query=keyword.replace(" ", "+"),
            municipio=localidade.replace(" ", "+"),
            date_filter=date_filter
        )
        print(f"🌐 URL gerada: {query_url}")
        
        try:
            async with session.get(query_url, headers=headers, timeout=30) as response:
                if response.status == 429:  # Too Many Requests
                    print("⚠️ Rate limit detected, implementing exponential backoff...")
                    raise Exception("Rate limit")
                
                content = await response.text()
                feed = feedparser.parse(content)
                print(f"🔎 {keyword} em {localidade} ({len(feed.entries)} notícias)")
                
                limite_dias = datetime.utcnow() - timedelta(days=dias)
                
                articles = []
                for item in feed.entries:
                    try:
                        published_date = item.get("published", "")
                        if published_date:
                            # Handle date format without timezone offset
                            try:
                                if "GMT" in published_date:
                                    # Replace GMT with +0000 for proper timezone parsing
                                    published_date = published_date.replace("GMT", "+0000")
                                pub_date = datetime.strptime(published_date, "%a, %d %b %Y %H:%M:%S %z").date()
                            except ValueError:
                                # Try alternative format if first attempt fails
                                try:
                                    pub_date = datetime.strptime(published_date.split(" +")[0], "%a, %d %b %Y %H:%M:%S").date()
                                except ValueError as e:
                                    print(f"⚠️ Error parsing article date: {published_date}")
                                    continue
                        else:
                            continue

                        if pub_date < limite_dias.date():
                            continue

                        articles.append({
                            "ID": gerar_id(item.get("link", "")),
                            "keyword": keyword,
                            "localidade": localidade,
                            "title": item.get("title", ""),
                            "link": item.get("link", ""),
                            "published": published_date
                        })
                    except (ValueError, IndexError) as e:
                        print(f"⚠️ Error parsing date for article: {e}")
                        continue
                
                # Cache the results
                CACHE[cache_key] = (time.time(), articles)
                return articles
                
        except Exception as e:
            print(f"❌ Error fetching {query_url}: {e}")
            raise  # Let retry decorator handle it

async def process_batch(session, batch, date_filter, dias):
    # Shuffle batch to make requests look more natural
    random.shuffle(batch)
    tasks = []
    for keyword, localidade in batch:
        tasks.append(fetch_news(session, keyword, localidade, date_filter, dias))
    return await asyncio.gather(*tasks)

async def run_scraper_async(specific_date=None, dias=1):
    # Clear cache at start
    CACHE.clear()
    
    date_filter = format_date_filter(specific_date) if specific_date else ""
    all_combinations = [(kw, loc) for loc in TODAS_LOCALIDADES for kw in KEYWORDS]
    random.shuffle(all_combinations)
    links_coletados = []
    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False, limit=20)) as session:
        for i in range(0, len(all_combinations), BATCH_SIZE):
            batch = all_combinations[i:i + BATCH_SIZE]
            print(f"📦 Processing batch {i//BATCH_SIZE + 1}/{len(all_combinations)//BATCH_SIZE + 1}")
            
            results = await process_batch(session, batch, date_filter, dias)
            for result in results:
                # links_coletados.extend(result)  # Optional: commented out to save memory
                if result:  # Save after each batch if there are any articles
                    save_intermediate_csv(result)
            
            # Reduced delay between batches
            if i + BATCH_SIZE < len(all_combinations):
                await asyncio.sleep(0.8)  # Decreased from 1.5
            
            # Reduced random delays
            if i + BATCH_SIZE < len(all_combinations) and random.random() < 0.1:  # Reduced probability from 0.2
                await asyncio.sleep(random.uniform(2, 4))  # Decreased from 3-7 range
    
    save_intermediate_csv(links_coletados)
    print(f"✅ Collection complete. Total: {len(links_coletados)} articles saved to {INTERMEDIATE_CSV}.")

def save_intermediate_csv(data):
    if not data:
        return
    os.makedirs(os.path.dirname(INTERMEDIATE_CSV), exist_ok=True)
    file_exists = os.path.isfile(INTERMEDIATE_CSV)
    with open(INTERMEDIATE_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ID", "keyword", "localidade", "title", "link", "published"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)

def main():
    parser = argparse.ArgumentParser(description="Google News Scraper")
    parser.add_argument(
        "--date", 
        help="Specific date to scrape (format: YYYY-MM-DD)",
        required=False
    )
    parser.add_argument(
        "--dias",
        type=int,
        default=1,
        help="Número de dias anteriores a considerar para as notícias (default: 1)"
    )
    args = parser.parse_args()
    
    if not args.dias:
        dias_input = input("⏳ Quantos dias anteriores queres considerar para o scraping? (default: 1): ")
        if dias_input.strip().isdigit():
            args.dias = int(dias_input)
        else:
            args.dias = 1
    
    print(f"📅 A processar artigos dos últimos {args.dias} dia(s)...")
    
    asyncio.run(run_scraper_async(specific_date=args.date, dias=args.dias))

if __name__ == "__main__":
    main()
