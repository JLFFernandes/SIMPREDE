#!/usr/bin/env python3
"""
Versão compatível com Airflow do run_scraper.py
Implementa a mesma estratégia de scraping com rate limiting e controle de concorrência
"""
import os
import csv
import hashlib
import feedparser
import sys
import time
import random
import asyncio
from datetime import datetime, timedelta
import argparse
import logging
from collections import defaultdict

# Import necessary modules with proper error handling - MOVED TO TOP
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, wait_random
    import aiohttp
except ImportError as e:
    print(f"Pacote necessário em falta: {e}")
    print("A instalar pacotes necessários...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tenacity", "aiohttp"])
    from tenacity import retry, stop_after_attempt, wait_exponential, wait_random
    import aiohttp

# Add parent directory to path to allow importing utils
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

from utils.helpers import load_keywords, carregar_paroquias_com_municipios, gerar_id
from pathlib import Path

# Configure logging for Airflow compatibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_scraper_airflow")

# Add statistics tracking for comprehensive logging
SCRAPING_STATS = {
    'total_requests': 0,
    'successful_requests': 0,
    'failed_requests': 0,
    'total_articles_found': 0,
    'total_articles_saved': 0,
    'rate_limits_hit': 0,
    'captcha_encounters': 0,
    'start_time': None,
    'batch_times': [],
    'domains_accessed': set(),
    'keywords_processed': set(),
    'locations_processed': set(),
    'cache_hits': 0,
    'cache_misses': 0
}

# Google News template and data loading
GOOGLE_NEWS_TEMPLATE = "https://news.google.com/rss/search?q={query}+{municipio}+Portugal&hl=pt-PT&gl=PT&ceid=PT:pt{date_filter}"
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")
LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")
TODAS_LOCALIDADES = list(LOCALIDADES.keys()) + MUNICIPIOS + DISTRITOS

# Advanced rate limiting settings - same as run_scraper.py
MAX_CONCURRENT_REQUESTS = 90
MIN_DELAY = 0.015
MAX_DELAY = 0.35
BATCH_SIZE = 120
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Dynamic concurrency control parameters - identical to run_scraper.py
CONCURRENCY_CONTROL = {
    'current_limit': MAX_CONCURRENT_REQUESTS,
    'min_limit': 40,
    'max_limit': 120,
    'success_streak': 0,
    'failure_streak': 0,
    'adjustment_threshold': 8,
    'last_adjustment': time.time()
}

# User agents and browser headers - same as run_scraper.py
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:108.0) Gecko/20100101 Firefox/108.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.55",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.55"
]

# Cache and domain rate limiting - same as run_scraper.py
CACHE = {}
CACHE_DURATION = 7200
DOMAINS_RATE_LIMITS = defaultdict(lambda: {'delay': 0.2, 'last_access': 0, 'success': 0, 'fail': 0})
RETRY_DELAY_MULTIPLIER = 1.3
SUCCESS_THRESHOLD = 3
PRIORITY_KEYWORDS = ["inundação", "tempestade", "chuva", "enchente", "alagamento"]

# Browser fingerprinting - same as run_scraper.py
BROWSER_HEADERS = [
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-PT,pt;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.57",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8,es;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    {
        "User-Agent": "Mozilla/5.0 (iPad; CPU OS 16_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Mobile/15E148 Safari/604.1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-PT,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
]

# Request patterns - same as run_scraper.py
REQUEST_PATTERNS = {
    "delays": {
        "between_requests": (0.1, 0.8),
        "between_batches": (0.3, 1.2),
        "error_cooldown": (2.0, 6.0),
        "captcha_cooldown": (20.0, 45.0)
    },
    "batch_variation": (0.92, 1.08)
}

# Cache optimization
MAX_CACHE_ITEMS = 20000
CACHE_CLEANUP_THRESHOLD = 0.20

class CacheEntry:
    __slots__ = ['timestamp', 'data']
    
    def __init__(self, timestamp, data):
        self.timestamp = timestamp
        self.data = data

def get_date_paths(specified_date=None):
    """Get appropriate date paths based on the specified date or current date"""
    if specified_date:
        try:
            dt = datetime.strptime(specified_date, "%Y-%m-%d")
            current_date = dt.strftime("%Y%m%d")
            current_year = dt.strftime("%Y")
            current_month = dt.strftime("%m")
            current_day = dt.strftime("%d")
        except ValueError as e:
            logger.error(f"Formato de data inválido: {e}")
            raise
    else:
        dt = datetime.now()
        current_date = dt.strftime("%Y%m%d")
        current_year = dt.strftime("%Y")
        current_month = dt.strftime("%m")
        current_day = dt.strftime("%d")
    
    raw_data_dir = os.path.join(PROJECT_ROOT, "data", "raw", current_year, current_month, current_day)
    intermediate_csv = os.path.join(raw_data_dir, f"intermediate_google_news_{current_date}.csv")
    
    os.makedirs(raw_data_dir, exist_ok=True)
    
    return raw_data_dir, intermediate_csv, current_date

def format_date_filter(date_str):
    """Format date filter for Google News URL."""
    if not date_str:
        return ""
    
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        return f"&when:1d"
    except ValueError:
        return ""

def log_scraping_statistics():
    """Log comprehensive scraping statistics for Airflow UI"""
    stats = SCRAPING_STATS
    elapsed_time = time.time() - stats['start_time'] if stats['start_time'] else 0
    
    logger.info("=" * 60)
    logger.info("📊 SCRAPING STATISTICS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"⏱️  Total Runtime: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
    logger.info(f"🔍 Total Requests: {stats['total_requests']}")
    logger.info(f"✅ Successful Requests: {stats['successful_requests']}")
    logger.info(f"❌ Failed Requests: {stats['failed_requests']}")
    
    if stats['total_requests'] > 0:
        success_rate = (stats['successful_requests'] / stats['total_requests']) * 100
        logger.info(f"📈 Success Rate: {success_rate:.1f}%")
        requests_per_second = stats['total_requests'] / elapsed_time if elapsed_time > 0 else 0
        logger.info(f"⚡ Request Rate: {requests_per_second:.2f} requests/second")
    
    logger.info(f"📰 Total Articles Found: {stats['total_articles_found']}")
    logger.info(f"💾 Total Articles Saved: {stats['total_articles_saved']}")
    logger.info(f"🚫 Rate Limits Hit: {stats['rate_limits_hit']}")
    logger.info(f"🤖 CAPTCHA Encounters: {stats['captcha_encounters']}")
    logger.info(f"🎯 Cache Hits: {stats['cache_hits']}")
    logger.info(f"💸 Cache Misses: {stats['cache_misses']}")
    
    if stats['cache_hits'] + stats['cache_misses'] > 0:
        cache_hit_rate = (stats['cache_hits'] / (stats['cache_hits'] + stats['cache_misses'])) * 100
        logger.info(f"📋 Cache Hit Rate: {cache_hit_rate:.1f}%")
    
    logger.info(f"🌐 Unique Domains: {len(stats['domains_accessed'])}")
    logger.info(f"🔑 Keywords Processed: {len(stats['keywords_processed'])}")
    logger.info(f"📍 Locations Processed: {len(stats['locations_processed'])}")
    
    if stats['batch_times']:
        avg_batch_time = sum(stats['batch_times']) / len(stats['batch_times'])
        logger.info(f"⏱️  Average Batch Time: {avg_batch_time:.1f} seconds")
        logger.info(f"🏃 Fastest Batch: {min(stats['batch_times']):.1f} seconds")
        logger.info(f"🐌 Slowest Batch: {max(stats['batch_times']):.1f} seconds")
    
    logger.info("=" * 60)

def update_scraping_stats(keyword=None, localidade=None, success=True, articles_count=0, 
                         rate_limited=False, captcha=False, cache_hit=False):
    """Update scraping statistics for logging"""
    stats = SCRAPING_STATS
    
    if success:
        stats['successful_requests'] += 1
    else:
        stats['failed_requests'] += 1
    
    stats['total_requests'] += 1
    stats['total_articles_found'] += articles_count
    
    if rate_limited:
        stats['rate_limits_hit'] += 1
    
    if captcha:
        stats['captcha_encounters'] += 1
    
    if cache_hit:
        stats['cache_hits'] += 1
    else:
        stats['cache_misses'] += 1
    
    if keyword:
        stats['keywords_processed'].add(keyword)
    
    if localidade:
        stats['locations_processed'].add(localidade)
    
    stats['domains_accessed'].add('news.google.com')

def adjust_domain_delay(domain, success):
    """Dynamically adjust delay based on success/failure rate - same as run_scraper.py"""
    domain_stats = DOMAINS_RATE_LIMITS[domain]
    
    if success:
        domain_stats['success'] += 1
        domain_stats['fail'] = max(0, domain_stats['fail'] - 1)
        
        if domain_stats['success'] >= SUCCESS_THRESHOLD:
            domain_stats['delay'] = max(MIN_DELAY, domain_stats['delay'] * 0.9)
            domain_stats['success'] = 0
    else:
        domain_stats['fail'] += 1
        domain_stats['success'] = 0
        
        if domain_stats['fail'] >= 2:
            domain_stats['delay'] = min(MAX_DELAY * 2, domain_stats['delay'] * RETRY_DELAY_MULTIPLIER)
            domain_stats['fail'] = 0

async def wait_for_domain(domain):
    """Wait appropriate time for a domain - same as run_scraper.py"""
    stats = DOMAINS_RATE_LIMITS[domain]
    current_time = time.time()
    elapsed = current_time - stats['last_access']
    
    if elapsed < stats['delay']:
        wait_time = stats['delay'] - elapsed
        wait_time += random.uniform(0, wait_time * 0.2)
        await asyncio.sleep(wait_time)
    
    jitter = random.uniform(-0.1, 0.1) * stats['delay']
    stats['last_access'] = time.time() + jitter

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=8) + wait_random(0, 2),
    reraise=True
)
async def fetch_news(session, keyword, localidade, date_filter, dias):
    """Fetch news articles - same strategy as run_scraper.py"""
    cache_key = f"{keyword}_{localidade}_{date_filter}_{dias}"
    
    # Check cache first
    if cache_key in CACHE:
        cache_entry = CACHE[cache_key]
        if (time.time() - cache_entry.timestamp) < CACHE_DURATION:
            update_scraping_stats(keyword, localidade, True, len(cache_entry.data), cache_hit=True)
            return cache_entry.data

    domain = "news.google.com"
    await wait_for_domain(domain)
    
    global SEMAPHORE
    if CONCURRENCY_CONTROL['current_limit'] != SEMAPHORE._value:
        SEMAPHORE = asyncio.Semaphore(CONCURRENCY_CONTROL['current_limit'])
    
    async with SEMAPHORE:
        browser_profile = random.choice(BROWSER_HEADERS)
        headers = browser_profile.copy()
        
        if random.random() > 0.5:
            headers["Cache-Control"] = random.choice(["max-age=0", "no-cache", "no-store"])
        
        if random.random() > 0.7:
            headers["TE"] = "Trailers"
            
        query_url = GOOGLE_NEWS_TEMPLATE.format(
            query=keyword.replace(" ", "+"),
            municipio=localidade.replace(" ", "+"),
            date_filter=date_filter
        )
        
        try:
            timeout_value = random.uniform(8, 15)
            timeout = aiohttp.ClientTimeout(total=timeout_value)
            
            request_start = time.time()
                
            async with session.get(query_url, headers=headers, timeout=timeout, 
                                  allow_redirects=True) as response:
                response_time = time.time() - request_start
                
                update_concurrency_control(response.status == 200, response_time)
                
                if response.status == 429:
                    logger.warning(f"🚫 Rate limit detected for {domain} - {keyword} em {localidade}")
                    adjust_domain_delay(domain, False)
                    update_scraping_stats(keyword, localidade, False, 0, rate_limited=True)
                    cooldown = random.uniform(*REQUEST_PATTERNS["delays"]["error_cooldown"])
                    await asyncio.sleep(cooldown)
                    raise Exception("Rate limit exceeded")
                
                if not response.ok:
                    logger.warning(f"❌ HTTP {response.status} for {keyword} em {localidade}")
                    adjust_domain_delay(domain, False)
                    update_scraping_stats(keyword, localidade, False, 0)
                    raise Exception(f"HTTP error {response.status}")
                
                content = await response.text()
                
                feed = feedparser.parse(content)
                if not feed or not hasattr(feed, 'entries') or len(feed.entries) == 0:
                    if "captcha" in content.lower():
                        logger.error(f"🤖 CAPTCHA detected for {keyword} em {localidade}! Implementing cooldown...")
                        adjust_domain_delay(domain, False)
                        update_scraping_stats(keyword, localidade, False, 0, captcha=True)
                        captcha_cooldown = random.uniform(*REQUEST_PATTERNS["delays"]["captcha_cooldown"])
                        logger.info(f"⏳ CAPTCHA cooldown: sleeping for {captcha_cooldown:.1f} seconds")
                        await asyncio.sleep(captcha_cooldown)
                        raise Exception("CAPTCHA detected")
                
                logger.info(f"🔎 {keyword} em {localidade} → {len(feed.entries)} notícias encontradas")
                
                adjust_domain_delay(domain, True)
                
                limite_dias = datetime.utcnow() - timedelta(days=dias)
                limite_superior = datetime.utcnow()
                
                articles = []
                for item in feed.entries:
                    pub_date = None
                    try:
                        published_date = item.get("published", "")
                        if not published_date:
                            continue
                            
                        date_formats = [
                            lambda d: datetime.strptime(d.replace("GMT", "+0000"), "%a, %d %b %Y %H:%M:%S %z"),
                            lambda d: datetime.strptime(d.split(" +")[0], "%a, %d %b %Y %H:%M:%S"),
                            lambda d: datetime.strptime(d, "%Y-%m-%dT%H:%M:%S"),
                            lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
                        ]
                        
                        for date_parser in date_formats:
                            try:
                                pub_date_obj = date_parser(published_date)
                                if hasattr(pub_date_obj, 'tzinfo') and pub_date_obj.tzinfo is not None:
                                    pub_date = pub_date_obj.replace(tzinfo=None).date()
                                else:
                                    pub_date = pub_date_obj.date()
                                break
                            except (ValueError, AttributeError, IndexError):
                                continue
                        
                        if pub_date is None:
                            logger.debug(f"⚠️ Could not parse date: {published_date}")
                            continue
                            
                        if pub_date < limite_dias.date() or pub_date > limite_superior.date():
                            continue

                        articles.append({
                            "ID": gerar_id(item.get("link", "")),
                            "keyword": keyword,
                            "localidade": localidade,
                            "title": item.get("title", ""),
                            "link": item.get("link", ""),
                            "published": published_date,
                            "collection_date": datetime.now().strftime("%Y-%m-%d")
                        })
                    except Exception as e:
                        logger.debug(f"⚠️ Error processing article: {e}")
                        continue
                
                # Log filtered results
                if articles:
                    logger.info(f"✅ {keyword} em {localidade} → {len(articles)} artigos válidos após filtros")
                
                if len(CACHE) > MAX_CACHE_ITEMS:
                    cleanup_count = int(MAX_CACHE_ITEMS * CACHE_CLEANUP_THRESHOLD)
                    old_keys = sorted(CACHE.keys(), key=lambda k: CACHE[k].timestamp)[:cleanup_count]
                    for k in old_keys:
                        CACHE.pop(k, None)
                    logger.debug(f"🧹 Cache cleanup: removed {cleanup_count} oldest entries")
                        
                CACHE[cache_key] = CacheEntry(time.time(), articles)
                update_scraping_stats(keyword, localidade, True, len(articles), cache_hit=False)
                return articles
                
        except asyncio.TimeoutError:
            logger.warning(f"⏰ Timeout fetching {keyword} em {localidade}")
            adjust_domain_delay(domain, False)
            update_scraping_stats(keyword, localidade, False, 0)
            raise
            
        except Exception as e:
            logger.warning(f"❌ Error fetching {keyword} em {localidade}: {e}")
            adjust_domain_delay(domain, False)
            update_scraping_stats(keyword, localidade, False, 0)
            raise

async def process_batch(session, batch, date_filter, dias):
    """Process a batch - same strategy as run_scraper.py"""
    random.shuffle(batch)
    
    concurrency = min(len(batch), MAX_CONCURRENT_REQUESTS)
    chunk_size = max(2, concurrency // 2)
    chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
    
    all_results = []
    for chunk_idx, chunk in enumerate(chunks):
        chunk_semaphore = asyncio.Semaphore(min(len(chunk), concurrency))
        
        async def process_item(keyword, localidade):
            async with chunk_semaphore:
                try:
                    return await fetch_news(session, keyword, localidade, date_filter, dias)
                except Exception as e:
                    logger.error(f"Error fetching news for {keyword} in {localidade}: {e}")
                    return []
        
        tasks = [process_item(keyword, localidade) for keyword, localidade in chunk]
        chunk_results = await asyncio.gather(*tasks, return_exceptions=False)
        
        chunk_results = [r for r in chunk_results if r]
        all_results.extend(chunk_results)
        
        if chunk_idx < len(chunks) - 1:
            chunk_delay = random.uniform(MIN_DELAY, MIN_DELAY * 4)
            logger.debug(f"Waiting {chunk_delay:.2f}s between chunks...")
            await asyncio.sleep(chunk_delay)
    
    return all_results

def prioritize_combinations(combinations, dias):
    """Prioritize combinations - same as run_scraper.py"""
    priority_factor = min(10, dias) / 10
    
    priority_combinations = []
    normal_combinations = []
    
    for kw, loc in combinations:
        if any(p_kw in kw.lower() for p_kw in PRIORITY_KEYWORDS):
            priority_combinations.append((kw, loc))
        else:
            normal_combinations.append((kw, loc))
    
    random.shuffle(priority_combinations)
    random.shuffle(normal_combinations)
    
    priority_count = int(len(priority_combinations) * priority_factor)
    
    return priority_combinations[:priority_count] + normal_combinations + priority_combinations[priority_count:]

def save_intermediate_csv(data, csv_path):
    """Save scraped data to CSV files"""
    if not data:
        return
    
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    file_exists = os.path.isfile(csv_path)
    try:
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["ID", "keyword", "localidade", "title", "link", "published", "collection_date"])
            if not file_exists:
                writer.writeheader()
            writer.writerows(data)
        
        # Update stats
        SCRAPING_STATS['total_articles_saved'] += len(data)
        logger.info(f"💾 Saved {len(data)} articles to {os.path.basename(csv_path)}")
    except Exception as e:
        logger.error(f"❌ Error saving to {csv_path}: {e}")

def update_concurrency_control(success, response_time):
    """Dynamically adjust concurrency limits - same logic as run_scraper.py"""
    control = CONCURRENCY_CONTROL
    current_time = time.time()
    
    if current_time - control['last_adjustment'] < 5:
        return
    
    if success:
        control['success_streak'] += 1
        control['failure_streak'] = 0
        
        if control['success_streak'] >= control['adjustment_threshold']:
            if response_time < 1.0 and control['current_limit'] < control['max_limit']:
                control['current_limit'] = min(control['max_limit'], 
                                              int(control['current_limit'] * 1.15))
                logger.info(f"⬆️ Increased concurrency to {control['current_limit']} (fast responses)")
            elif response_time < 2.0 and control['current_limit'] < control['max_limit']:
                control['current_limit'] = min(control['max_limit'], 
                                              control['current_limit'] + 5)
                logger.info(f"⬆️ Increased concurrency to {control['current_limit']} (good performance)")
            
            control['success_streak'] = 0
            control['last_adjustment'] = current_time
    else:
        control['failure_streak'] += 1
        control['success_streak'] = 0
        
        if control['failure_streak'] >= control['adjustment_threshold'] // 2:
            control['current_limit'] = max(control['min_limit'], 
                                          int(control['current_limit'] * 0.8))
            logger.warning(f"⬇️ Reduced concurrency to {control['current_limit']} due to failures")
            control['failure_streak'] = 0
            control['last_adjustment'] = current_time
        elif response_time > 5.0:
            control['current_limit'] = max(control['min_limit'], 
                                          int(control['current_limit'] * 0.9))
            logger.info(f"⬇️ Reduced concurrency to {control['current_limit']} due to slow responses")
            control['last_adjustment'] = current_time

async def main_async(days_back=1, search_date=None):
    """Main async function with enhanced Airflow logging"""
    # Initialize statistics
    SCRAPING_STATS['start_time'] = time.time()
    
    logger.info("🚀 Starting Google News Scraper (Airflow Version)")
    logger.info(f"📅 Search parameters: {days_back} days back, specific date: {search_date}")
    
    CACHE.clear()
    
    raw_data_dir, intermediate_csv, current_date = get_date_paths(search_date)
    logger.info(f"📁 Output directory: {raw_data_dir}")
    logger.info(f"📄 Output file: {os.path.basename(intermediate_csv)}")
    
    date_filter = format_date_filter(search_date) if search_date else ""
    
    all_combinations = [(kw, loc) for loc in TODAS_LOCALIDADES for kw in KEYWORDS]
    all_combinations = prioritize_combinations(all_combinations, days_back)
    
    total_combinations = len(all_combinations)
    logger.info(f"🎯 Total keyword-location combinations: {total_combinations}")
    logger.info(f"📍 Locations: {len(TODAS_LOCALIDADES)}")
    logger.info(f"🔑 Keywords: {len(KEYWORDS)}")
    
    # For Airflow, use smaller batches to avoid timeouts
    if total_combinations > 5000:
        adjusted_batch_size = min(BATCH_SIZE // 2, max(20, int(BATCH_SIZE // 2)))
    elif total_combinations > 2000:
        adjusted_batch_size = min(BATCH_SIZE // 1.5, max(30, int(BATCH_SIZE * 0.6)))
    elif total_combinations > 1000:
        adjusted_batch_size = min(BATCH_SIZE, max(40, int(BATCH_SIZE * 0.8)))
    else:
        adjusted_batch_size = BATCH_SIZE
    
    logger.info(f"📦 Using Airflow-optimized batch size: {adjusted_batch_size}")
    logger.info(f"🔄 Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    
    # Log system information for debugging
    logger.info(f"🖥️ System: Python {sys.version}")
    logger.info(f"📊 Memory usage tracking enabled")
    
    tcp_connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS * 2,  # Reduced for Airflow
        ttl_dns_cache=600,  # Reduced DNS cache for Airflow
        enable_cleanup_closed=True,
        force_close=False,
        ssl=False,
        limit_per_host=random.randint(10, 15)  # Reduced for Airflow
    )
    
    connect_timeout = random.uniform(3, 5)  # Reduced for Airflow
    sock_read_timeout = random.uniform(6, 10)  # Reduced for Airflow
    total_timeout = connect_timeout + sock_read_timeout + random.uniform(1, 3)
    
    timeout = aiohttp.ClientTimeout(
        total=total_timeout, 
        connect=connect_timeout,
        sock_read=sock_read_timeout
    )
    
    logger.info(f"⏱️ Request timeout: {total_timeout:.1f}s (Airflow optimized)")
    
    try:
        async with aiohttp.ClientSession(connector=tcp_connector, timeout=timeout) as session:
            processed_combinations = 0
            
            for i in range(0, total_combinations, adjusted_batch_size):
                # Check if we should break early for Airflow timeout management
                elapsed_time = time.time() - SCRAPING_STATS['start_time']
                if elapsed_time > 300:  # 5 minutes - leave buffer for Airflow
                    logger.warning(f"⚠️ Approaching Airflow timeout limit. Processed {processed_combinations}/{total_combinations} combinations")
                    break
                
                variation = random.uniform(*REQUEST_PATTERNS["batch_variation"])
                current_batch_size = min(
                    int(adjusted_batch_size * variation),
                    total_combinations - i
                )
                
                batch = all_combinations[i:i + current_batch_size]
                batch_number = i // adjusted_batch_size + 1
                total_batches = (total_combinations + adjusted_batch_size - 1) // adjusted_batch_size
                
                percent_complete = (i / total_combinations) * 100
                logger.info("=" * 50)
                logger.info(f"📦 BATCH {batch_number}/{total_batches} ({percent_complete:.1f}% complete)")
                logger.info(f"🎯 Processing {current_batch_size} combinations")
                logger.info(f"⏱️ Elapsed time: {elapsed_time:.1f}s")
                
                batch_start_time = time.time()
                
                try:
                    results = await process_batch(session, batch, date_filter, days_back)
                    batch_end_time = time.time()
                    batch_duration = batch_end_time - batch_start_time
                    
                    # Track batch timing
                    SCRAPING_STATS['batch_times'].append(batch_duration)
                    
                    articles = [article for result in results for article in result]
                    if articles:
                        save_intermediate_csv(articles, intermediate_csv)
                        logger.info(f"✅ Batch {batch_number} complete: {len(articles)} articles saved")
                    else:
                        logger.info(f"ℹ️ Batch {batch_number} complete: No new articles found")
                    
                    logger.info(f"⏱️ Batch duration: {batch_duration:.1f}s")
                    
                    # Log current statistics every few batches
                    if batch_number % 5 == 0 or batch_number == total_batches:
                        logger.info(f"📊 Progress: {SCRAPING_STATS['successful_requests']}/{SCRAPING_STATS['total_requests']} requests successful")
                        logger.info(f"📰 Total articles so far: {SCRAPING_STATS['total_articles_saved']}")
                        logger.info(f"🎯 Cache hit rate: {SCRAPING_STATS['cache_hits']}/{SCRAPING_STATS['cache_hits'] + SCRAPING_STATS['cache_misses']}")
                    
                    processed_combinations += current_batch_size
                    
                except Exception as batch_error:
                    logger.error(f"❌ Batch {batch_number} failed: {batch_error}")
                    continue
                
                if i + current_batch_size < total_combinations:
                    batch_time = batch_duration
                    
                    min_delay, max_delay = REQUEST_PATTERNS["delays"]["between_batches"]
                    
                    # Shorter delays for Airflow to stay within timeout
                    if batch_time < 3:
                        delay = random.uniform(0.5, 1.0)
                    elif batch_time < 10:
                        delay = random.uniform(0.3, 0.8)
                    else:
                        delay = random.uniform(0.1, 0.5)
                    
                    remaining_batches = total_batches - batch_number
                    est_time_remaining = int(remaining_batches * (batch_time + delay))
                    
                    logger.info(f"⏳ Waiting {delay:.2f}s before next batch...")
                    logger.info(f"⏰ Est. time remaining: {est_time_remaining//60}m {est_time_remaining%60}s")
                    
                    # Check if estimated time would exceed Airflow limits
                    if elapsed_time + est_time_remaining > 400:  # 6.5 minutes buffer
                        logger.warning(f"⚠️ Would exceed Airflow timeout. Stopping early.")
                        break
                    
                    await asyncio.sleep(delay)
    
    except Exception as e:
        logger.error(f"💥 Session error: {e}")
        raise
    
    logger.info("🎉 SCRAPING PROCESS COMPLETED!")
    logger.info(f"📊 Processed {processed_combinations}/{total_combinations} combinations")
    
    # Log final comprehensive statistics
    log_scraping_statistics()
    
    # Cache cleanup
    target_size = 1000  # Smaller for Airflow
    if len(CACHE) > target_size:
        cache_keys = list(CACHE.keys())
        cache_keys.sort(key=lambda k: CACHE[k].timestamp, reverse=True)
        keep_keys = set(cache_keys[:target_size])
        for k in list(CACHE.keys()):
            if k not in keep_keys:
                CACHE.pop(k, None)
        logger.info(f"🧹 Cache optimized: keeping {len(keep_keys)} most recent entries")
    
    return SCRAPING_STATS['total_articles_saved']

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Scraper de Google News para Airflow")
    parser.add_argument("--dias", type=int, default=1, help="Número de dias anteriores a considerar")
    parser.add_argument("--date", type=str, help="Data específica para pesquisar (AAAA-MM-DD)")
    
    args = parser.parse_args()
    
    logger.info("🚀 Iniciando o Scraper de Google News (Versão Airflow)")
    logger.info(f"⚙️ Parâmetros: {args.dias} dias, data específica: {args.date}")
    
    if args.date:
        try:
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"❌ Formato de data inválido: {args.date}. Use AAAA-MM-DD")
            sys.exit(1)
    
    try:
        result = asyncio.run(main_async(args.dias, args.date))
        logger.info(f"🎉 Scraper concluído com sucesso!")
        logger.info(f"📰 Total de artigos coletados: {result}")
        return 0
    except Exception as e:
        logger.error(f"💥 Erro ao executar o scraper: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
