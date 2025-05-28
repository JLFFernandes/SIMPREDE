import os
import csv
import hashlib
import feedparser
import sys
import time
import random
import asyncio
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random
import aiohttp
from datetime import datetime, timedelta
import argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.helpers import load_keywords, carregar_paroquias_com_municipios, gerar_id
from pathlib import Path
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_scraper")

# Update template to include date parameters
GOOGLE_NEWS_TEMPLATE = "https://news.google.com/rss/search?q={query}+{municipio}+Portugal&hl=pt-PT&gl=PT&ceid=PT:pt{date_filter}"
KEYWORDS = load_keywords("config/keywords.json", idioma="portuguese")
LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios("config/municipios_por_distrito.json")
TODAS_LOCALIDADES = list(LOCALIDADES.keys()) + MUNICIPIOS + DISTRITOS

# Use date in the intermediate CSV filename
current_date = datetime.now().strftime("%Y%m%d")
current_year = datetime.now().strftime("%Y")
current_month = datetime.now().strftime("%m")
current_day = datetime.now().strftime("%d")

# Define paths with year/month/day structure
RAW_DATA_DIR = os.path.join("data", "raw", current_year, current_month, current_day)
INTERMEDIATE_CSV = os.path.join(RAW_DATA_DIR, f"intermediate_google_news_{current_date}.csv")
# For backward compatibility
DEFAULT_INTERMEDIATE_CSV = "data/raw/intermediate_google_news.csv"

# Improved rate limiting settings - more adaptive
MAX_CONCURRENT_REQUESTS = 90  # Increased for optimal throughput
MIN_DELAY = 0.015  # Further reduced for faster operation
MAX_DELAY = 0.35   # Reduced for better speed
BATCH_SIZE = 120  # Optimized batch size for better efficiency

# Initial semaphore value - will be adjusted dynamically
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Dynamic concurrency control parameters
CONCURRENCY_CONTROL = {
    'current_limit': MAX_CONCURRENT_REQUESTS,
    'min_limit': 40,  # Lower bound for concurrency
    'max_limit': 120,  # Upper bound for concurrency
    'success_streak': 0,  # Track successful requests
    'failure_streak': 0,  # Track failed requests
    'adjustment_threshold': 8,  # How many successes/failures before adjusting
    'last_adjustment': time.time()
}

# Add new constants
USER_AGENTS = UserAgent(browsers=['chrome', 'firefox', 'safari', 'edge'])
CACHE = {}
CACHE_DURATION = 7200  # 2 hours cache
DOMAINS_RATE_LIMITS = defaultdict(lambda: {'delay': 0.2, 'last_access': 0, 'success': 0, 'fail': 0})
RETRY_DELAY_MULTIPLIER = 1.3  # Reduced multiplier for retry delay
SUCCESS_THRESHOLD = 3  # Fewer successes before reducing delay

# Proxy rotation (add this for distributed requests)
PROXY_LIST = []  # Fill this with proxies if you have them
USE_PROXIES = False  # Set to True when you have proxies configured

# Prioritize keywords for more relevant results
PRIORITY_KEYWORDS = ["inundação", "tempestade", "chuva", "enchente", "alagamento"]

# Browser fingerprinting variation to avoid detection
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

# Optimized request patterns to appear more human but run faster
REQUEST_PATTERNS = {
    "delays": {
        "between_requests": (0.1, 0.8),   # Further reduced delay between individual requests
        "between_batches": (0.3, 1.2),    # Further reduced delay between batches
        "error_cooldown": (2.0, 6.0),     # Shorter cooldown after errors
        "captcha_cooldown": (20.0, 45.0)  # Optimized cooldown after CAPTCHA
    },
    "batch_variation": (0.92, 1.08)       # Less variation for more predictable performance
}

# Advanced cache with memory optimization
MAX_CACHE_ITEMS = 20000  # Increased cache size for better performance
CACHE_CLEANUP_THRESHOLD = 0.20  # Clean up 20% of oldest cache entries when threshold reached

# Memory-efficient caching system
class CacheEntry:
    __slots__ = ['timestamp', 'data']
    
    def __init__(self, timestamp, data):
        self.timestamp = timestamp
        self.data = data

# Initialize with optimized memory usage
CACHE = {}

def format_date_filter(date_str):
    """Format date filter for Google News URL."""
    if not date_str:
        return ""
    
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        # Google News uses 'when:Xd' format for last X days
        return f"&when:1d"  # Just a single day
    except ValueError:
        return ""

def update_concurrency_control(success, response_time):
    """Dynamically adjust concurrency limits based on response time and success"""
    control = CONCURRENCY_CONTROL
    current_time = time.time()
    
    # Only make adjustments periodically
    if current_time - control['last_adjustment'] < 5:  # Only adjust every 5 seconds at most
        return
    
    if success:
        control['success_streak'] += 1
        control['failure_streak'] = 0
        
        # If we're having consistent success and responses are fast, increase concurrency
        if control['success_streak'] >= control['adjustment_threshold']:
            if response_time < 1.0 and control['current_limit'] < control['max_limit']:
                # If responses are very fast, we can be more aggressive
                control['current_limit'] = min(control['max_limit'], 
                                              int(control['current_limit'] * 1.15))
                logger.debug(f"⬆️ Increased concurrency to {control['current_limit']}")
            elif response_time < 2.0 and control['current_limit'] < control['max_limit']:
                # Modest increase for good performance
                control['current_limit'] = min(control['max_limit'], 
                                              control['current_limit'] + 5)
                logger.debug(f"⬆️ Increased concurrency to {control['current_limit']}")
            
            control['success_streak'] = 0
            control['last_adjustment'] = current_time
    else:
        control['failure_streak'] += 1
        control['success_streak'] = 0
        
        # If we're having failures or responses are slow, decrease concurrency
        if control['failure_streak'] >= control['adjustment_threshold'] // 2:  # React faster to failures
            # More aggressive reduction for failures
            control['current_limit'] = max(control['min_limit'], 
                                          int(control['current_limit'] * 0.8))
            logger.warning(f"⬇️ Reduced concurrency to {control['current_limit']} due to failures")
            control['failure_streak'] = 0
            control['last_adjustment'] = current_time
        elif response_time > 5.0:
            # Responses are too slow, reduce concurrency
            control['current_limit'] = max(control['min_limit'], 
                                          int(control['current_limit'] * 0.9))
            logger.debug(f"⬇️ Reduced concurrency to {control['current_limit']} due to slow responses")
            control['last_adjustment'] = current_time

def adjust_domain_delay(domain, success):
    """Dynamically adjust delay based on success/failure rate"""
    domain_stats = DOMAINS_RATE_LIMITS[domain]
    
    if success:
        domain_stats['success'] += 1
        domain_stats['fail'] = max(0, domain_stats['fail'] - 1)
        
        # After several consecutive successes, gradually reduce delay
        if domain_stats['success'] >= SUCCESS_THRESHOLD:
            domain_stats['delay'] = max(MIN_DELAY, domain_stats['delay'] * 0.9)
            domain_stats['success'] = 0  # Reset counter
    else:
        domain_stats['fail'] += 1
        domain_stats['success'] = 0
        
        # Increase delay after failures
        if domain_stats['fail'] >= 2:
            domain_stats['delay'] = min(MAX_DELAY * 2, domain_stats['delay'] * RETRY_DELAY_MULTIPLIER)
            domain_stats['fail'] = 0  # Reset counter

async def wait_for_domain(domain):
    """Wait appropriate time for a domain based on previous request history"""
    stats = DOMAINS_RATE_LIMITS[domain]
    current_time = time.time()
    elapsed = current_time - stats['last_access']
    
    if elapsed < stats['delay']:
        wait_time = stats['delay'] - elapsed
        # Add small random variation to avoid detection patterns
        wait_time += random.uniform(0, wait_time * 0.2)
        await asyncio.sleep(wait_time)
    
    # Vary delay slightly with each request
    jitter = random.uniform(-0.1, 0.1) * stats['delay']
    stats['last_access'] = time.time() + jitter

@retry(
    stop=stop_after_attempt(4),  # Try up to 4 times
    wait=wait_exponential(multiplier=1, min=2, max=8) + wait_random(0, 2),  # Add randomness to delays
    reraise=True
)
async def fetch_news(session, keyword, localidade, date_filter, dias):
    """Fetch news articles with improved caching and error handling"""
    cache_key = f"{keyword}_{localidade}_{date_filter}_{dias}"
    
    # Check cache first
    if cache_key in CACHE:
        cache_entry = CACHE[cache_key]
        if (time.time() - cache_entry.timestamp) < CACHE_DURATION:
            return cache_entry.data

    # Apply domain-specific rate limiting
    domain = "news.google.com"
    await wait_for_domain(domain)
    
    # Update concurrency control based on global success/failure patterns
    global SEMAPHORE
    if CONCURRENCY_CONTROL['current_limit'] != SEMAPHORE._value:
        # Create a new semaphore with the current limit
        SEMAPHORE = asyncio.Semaphore(CONCURRENCY_CONTROL['current_limit'])
    
    async with SEMAPHORE:
        # Make request appear more human-like with varied headers
        # Choose a random browser fingerprint from our collection
        browser_profile = random.choice(BROWSER_HEADERS)
        
        # Modify a few parameters randomly to appear more organic
        headers = browser_profile.copy()
        
        # Add small random variations to make each request unique
        if random.random() > 0.5:
            headers["Cache-Control"] = random.choice(["max-age=0", "no-cache", "no-store"])
        
        if random.random() > 0.7:
            headers["TE"] = "Trailers"
            
        # Create URL with quoted parameters for better compatibility
        query_url = GOOGLE_NEWS_TEMPLATE.format(
            query=keyword.replace(" ", "+"),
            municipio=localidade.replace(" ", "+"),
            date_filter=date_filter
        )
        
        try:
            # Use dynamic timeout with slight variations
            timeout_value = random.uniform(8, 15)  # Further optimized timeout values
            timeout = aiohttp.ClientTimeout(total=timeout_value)
            
            # Add intelligent retry countdown
            retry_attempt = 0
            max_retries = 3
            
            # Use proxy if available and enabled
            proxy = None
            if USE_PROXIES and PROXY_LIST:
                proxy = random.choice(PROXY_LIST)
            
            request_start = time.time()
                
            async with session.get(query_url, headers=headers, timeout=timeout, 
                                  proxy=proxy, allow_redirects=True) as response:
                response_time = time.time() - request_start
                
                # Update global concurrency controls based on response time
                update_concurrency_control(response.status == 200, response_time)
                
                if response.status == 429:  # Too Many Requests
                    logger.warning(f"Rate limit detected for {domain}, implementing backoff...")
                    adjust_domain_delay(domain, False)  # Adjust delay on failure
                    # Use the predefined cooldown pattern for rate limiting
                    cooldown = random.uniform(*REQUEST_PATTERNS["delays"]["error_cooldown"])
                    await asyncio.sleep(cooldown)
                    raise Exception("Rate limit exceeded")
                
                if not response.ok:
                    logger.warning(f"Non-200 response: {response.status} for {query_url}")
                    adjust_domain_delay(domain, False)
                    raise Exception(f"HTTP error {response.status}")
                
                content = await response.text()
                
                # Parse feed with error handling
                feed = feedparser.parse(content)
                if not feed or not hasattr(feed, 'entries') or len(feed.entries) == 0:
                    if "captcha" in content.lower():
                        logger.error("CAPTCHA detected! Need to slow down requests.")
                        adjust_domain_delay(domain, False)
                        # Use the predefined cooldown pattern for CAPTCHA
                        captcha_cooldown = random.uniform(*REQUEST_PATTERNS["delays"]["captcha_cooldown"])
                        logger.info(f"CAPTCHA cooldown: sleeping for {captcha_cooldown:.1f} seconds")
                        await asyncio.sleep(captcha_cooldown)
                        raise Exception("CAPTCHA detected")
                
                logger.info(f"🔎 {keyword} em {localidade} ({len(feed.entries)} notícias)")
                
                # Successful request - adjust rate limit stats
                adjust_domain_delay(domain, True)
                
                # Date filtering
                limite_dias = datetime.utcnow() - timedelta(days=dias)
                limite_superior = datetime.utcnow()
                
                articles = []
                for item in feed.entries:
                    pub_date = None  # Initialize pub_date
                    try:
                        published_date = item.get("published", "")
                        if not published_date:
                            continue
                            
                        # Try multiple date formats
                        date_formats = [
                            # With timezone
                            lambda d: datetime.strptime(d.replace("GMT", "+0000"), "%a, %d %b %Y %H:%M:%S %z"),
                            # Without timezone
                            lambda d: datetime.strptime(d.split(" +")[0], "%a, %d %b %Y %H:%M:%S"),
                            # ISO format
                            lambda d: datetime.strptime(d, "%Y-%m-%dT%H:%M:%S"),
                            # Additional common formats
                            lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
                        ]
                        
                        for date_parser in date_formats:
                            try:
                                pub_date_obj = date_parser(published_date)
                                if hasattr(pub_date_obj, 'tzinfo') and pub_date_obj.tzinfo is not None:
                                    pub_date = pub_date_obj.replace(tzinfo=None).date()
                                else:
                                    pub_date = pub_date_obj.date()
                                break  # Successfully parsed date
                            except (ValueError, AttributeError, IndexError):
                                continue  # Try next format
                        
                        # Skip if no valid date was parsed
                        if pub_date is None:
                            logger.debug(f"⚠️ Could not parse date: {published_date}")
                            continue
                            
                        # Strict date filtering based on user input
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
                
                # Limit cache size to prevent memory issues
                if len(CACHE) > MAX_CACHE_ITEMS:
                    # Remove oldest cache entries more efficiently
                    cleanup_count = int(MAX_CACHE_ITEMS * CACHE_CLEANUP_THRESHOLD)
                    old_keys = sorted(CACHE.keys(), key=lambda k: CACHE[k].timestamp)[:cleanup_count]
                    for k in old_keys:
                        CACHE.pop(k, None)
                    logger.debug(f"Cache cleanup: removed {cleanup_count} oldest entries")
                        
                # Cache the results with efficient storage
                CACHE[cache_key] = CacheEntry(time.time(), articles)
                return articles
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching {query_url}")
            adjust_domain_delay(domain, False)
            raise  # Let retry handle it
            
        except Exception as e:
            logger.warning(f"❌ Error fetching {query_url}: {e}")
            adjust_domain_delay(domain, False)
            raise  # Let retry handle it

async def process_batch(session, batch, date_filter, dias):
    """Process a batch of keyword-location combinations with improved handling"""
    # Shuffle batch for more natural request patterns
    random.shuffle(batch)
    
    # Implement dynamic batch sizing with partitioning for better concurrency control
    concurrency = min(len(batch), MAX_CONCURRENT_REQUESTS)
    
    # Divide batch into smaller chunks for more natural access patterns - use larger chunks for speed
    chunk_size = max(2, concurrency // 2)  # Increased minimum chunk size to 2 for better efficiency
    chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
    
    all_results = []
    for chunk_idx, chunk in enumerate(chunks):
        # Use a semaphore to control max concurrent requests per chunk
        chunk_semaphore = asyncio.Semaphore(min(len(chunk), concurrency))
        
        # Process chunk with controlled concurrency
        async def process_item(keyword, localidade):
            async with chunk_semaphore:
                try:
                    return await fetch_news(session, keyword, localidade, date_filter, dias)
                except Exception as e:
                    logger.error(f"Error fetching news for {keyword} in {localidade}: {e}")
                    return []
        
        tasks = [process_item(keyword, localidade) for keyword, localidade in chunk]
        chunk_results = await asyncio.gather(*tasks, return_exceptions=False)
        
        # Filter out empty results
        chunk_results = [r for r in chunk_results if r]
        all_results.extend(chunk_results)
        
        # Add shorter natural delay between chunks to appear more human-like but faster
        if chunk_idx < len(chunks) - 1:  # Don't delay after the last chunk
            # Use shorter delays for faster processing
            chunk_delay = random.uniform(MIN_DELAY, MIN_DELAY * 4)
            # Only log at debug level to reduce console output
            logger.debug(f"Waiting {chunk_delay:.2f}s between chunks...")
            await asyncio.sleep(chunk_delay)
    
    return all_results

def prioritize_combinations(combinations, dias):
    """Prioritize combinations for more efficient scraping"""
    # Prioritize more recent disasters (higher days value = higher priority)
    priority_factor = min(10, dias) / 10
    
    # First process combinations with priority keywords
    priority_combinations = []
    normal_combinations = []
    
    for kw, loc in combinations:
        if any(p_kw in kw.lower() for p_kw in PRIORITY_KEYWORDS):
            priority_combinations.append((kw, loc))
        else:
            normal_combinations.append((kw, loc))
    
    # Shuffle each group for randomness
    random.shuffle(priority_combinations)
    random.shuffle(normal_combinations)
    
    # Determine how many priority combos to process based on days parameter
    priority_count = int(len(priority_combinations) * priority_factor)
    
    # Return prioritized combinations first, then remaining ones
    return priority_combinations[:priority_count] + normal_combinations + priority_combinations[priority_count:]

async def run_scraper_async(specific_date=None, dias=1):
    """Main async function to run the scraper with improved efficiency and stealth"""
    # Clear cache at start
    CACHE.clear()
    
    date_filter = format_date_filter(specific_date) if specific_date else ""
    
    # Generate all combinations
    all_combinations = [(kw, loc) for loc in TODAS_LOCALIDADES for kw in KEYWORDS]
    
    # Apply intelligent prioritization and filtering
    all_combinations = prioritize_combinations(all_combinations, dias)
    
    # Calculate an optimized batch size based on the total workload
    total_combinations = len(all_combinations)
    
    # Dynamically adjust batch size based on total combinations using a smarter algorithm
    # This balances speed and detection risk more effectively
    if total_combinations > 5000:
        # For very large workloads, use moderate batch sizes to avoid detection
        adjusted_batch_size = min(BATCH_SIZE, max(30, int(BATCH_SIZE // 1.5)))
    elif total_combinations > 2000:
        # For large workloads, use slightly larger batches for better efficiency
        adjusted_batch_size = min(BATCH_SIZE, max(40, int(BATCH_SIZE * 0.8)))
    elif total_combinations > 1000:
        # For medium workloads, use optimized batch size for good performance
        adjusted_batch_size = min(BATCH_SIZE, max(50, int(BATCH_SIZE * 0.9)))
    else:
        # For small workloads, use full batch size for maximum speed
        adjusted_batch_size = BATCH_SIZE
    
    logger.info(f"Starting to process {total_combinations} combinations with batch size {adjusted_batch_size}...")
    
    # Configure session with optimized connection pooling
    tcp_connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS * 3,  # Further increased limit for better throughput
        ttl_dns_cache=1200,  # Increased DNS cache time to 20 minutes for better performance
        enable_cleanup_closed=True,
        force_close=False,
        ssl=False,
        # Use a higher connections per host limit for faster processing
        limit_per_host=random.randint(15, 20)  # Increased connection limit per host
    )
    
    # Use faster timeouts while still maintaining reliability
    connect_timeout = random.uniform(4, 6)  # Reduced connection timeout
    sock_read_timeout = random.uniform(8, 12)  # Optimized read timeout
    total_timeout = connect_timeout + sock_read_timeout + random.uniform(2, 4)  # Reduced buffer
    
    timeout = aiohttp.ClientTimeout(
        total=total_timeout, 
        connect=connect_timeout,
        sock_read=sock_read_timeout
    )
    
    async with aiohttp.ClientSession(connector=tcp_connector, timeout=timeout) as session:
        # Process in dynamic batches with human-like patterns
        for i in range(0, total_combinations, adjusted_batch_size):
            # Apply batch size variation to avoid predictable patterns
            variation = random.uniform(*REQUEST_PATTERNS["batch_variation"])
            current_batch_size = min(
                int(adjusted_batch_size * variation),  # Ensure integer for batch size
                total_combinations - i
            )
            
            batch = all_combinations[i:i + current_batch_size]
            batch_number = i // adjusted_batch_size + 1
            total_batches = (total_combinations + adjusted_batch_size - 1) // adjusted_batch_size
            
            # Include percentage complete for better progress tracking
            percent_complete = (i / total_combinations) * 100
            logger.info(f"📦 Processing batch {batch_number}/{total_batches} ({percent_complete:.1f}% complete)")
            
            # Process batch and save results
            batch_start_time = time.time()
            results = await process_batch(session, batch, date_filter, dias)
            
            # Flatten results and save
            articles = [article for result in results for article in result]
            if articles:
                save_intermediate_csv(articles)
                logger.info(f"✅ Saved {len(articles)} articles from batch {batch_number}")
            
            # Dynamic delay between batches that mimics human behavior
            if i + current_batch_size < total_combinations:
                # Calculate smart delay based on batch processing time
                batch_time = time.time() - batch_start_time
                
                # Longer delays for quicker batches (suspicious), shorter for slower ones
                min_delay, max_delay = REQUEST_PATTERNS["delays"]["between_batches"]
                
                # Inverse relationship - faster batches get longer delays
                if batch_time < 5:
                    delay = random.uniform(max(min_delay, 2.0), max_delay)
                elif batch_time < 15:
                    delay = random.uniform(min_delay, min(max_delay, 2.0))
                else:
                    delay = random.uniform(min_delay/2, min_delay)
                
                # Add small random variation
                delay_factor = random.uniform(0.85, 1.15)
                delay = delay * delay_factor
                
                # Progress indicator with time estimate
                remaining_batches = total_batches - batch_number
                est_time_remaining = int(remaining_batches * (batch_time + delay))
                
                logger.info(f"Waiting {delay:.2f}s before next batch... Est. time remaining: {est_time_remaining//60}m {est_time_remaining%60}s")
                await asyncio.sleep(delay)
    
    # Clean up and report completion
    logger.info(f"✅ Collection complete. Processed {total_combinations} combinations.")
    logger.info(f"📊 Cache stats: {len(CACHE)} entries")
    logger.info(f"📄 Results saved to {INTERMEDIATE_CSV}")
    
    # Run a memory-optimized cache cleanup
    gc_before = len(CACHE)
    # Keep only the most recent entries to improve future runs
    target_size = 1500  # Increased from 1000 to 1500 for better caching
    if len(CACHE) > target_size:
        cache_keys = list(CACHE.keys())
        # Sort by timestamp (most recent first)
        cache_keys.sort(key=lambda k: CACHE[k][0], reverse=True)
        # Keep only the most recent entries
        keep_keys = set(cache_keys[:target_size])
        # Remove older entries
        for k in list(CACHE.keys()):
            if k not in keep_keys:
                CACHE.pop(k, None)
    logger.info(f"🧹 Cache optimization: {gc_before} → {len(CACHE)} entries")

def save_intermediate_csv(data):
    """Save scraped data to CSV files with improved handling"""
    if not data:
        return
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(INTERMEDIATE_CSV), exist_ok=True)
    
    # Save to date-specific file
    file_exists = os.path.isfile(INTERMEDIATE_CSV)
    try:
        with open(INTERMEDIATE_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["ID", "keyword", "localidade", "title", "link", "published", "collection_date"])
            if not file_exists:
                writer.writeheader()
            writer.writerows(data)
    except Exception as e:
        logger.error(f"Error saving to {INTERMEDIATE_CSV}: {e}")
    
    # Also save to default file for backward compatibility
    file_exists = os.path.isfile(DEFAULT_INTERMEDIATE_CSV)
    try:
        with open(DEFAULT_INTERMEDIATE_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["ID", "keyword", "localidade", "title", "link", "published", "collection_date"])
            if not file_exists:
                writer.writeheader()
            writer.writerows(data)
    except Exception as e:
        logger.error(f"Error saving to {DEFAULT_INTERMEDIATE_CSV}: {e}")

def main():
    """Main function to run the scraper"""
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
    parser.add_argument(
        "--quiet", 
        action="store_true",
        help="Run with minimal output"
    )
    args = parser.parse_args()
    
    # Configure logging level based on quiet flag
    if args.quiet:
        logger.setLevel(logging.WARNING)
    
    if not args.dias:
        dias_input = input("⏳ Quantos dias anteriores queres considerar para o scraping? (default: 1): ")
        if dias_input.strip().isdigit():
            args.dias = int(dias_input)
        else:
            args.dias = 1
    
    logger.info(f"📅 A processar artigos dos últimos {args.dias} dia(s)...")
    logger.info(f"📄 Resultados serão guardados em: {INTERMEDIATE_CSV}")
    
    # Run with optimized asyncio settings
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        # Run with higher task limit
        asyncio.run(run_scraper_async(specific_date=args.date, dias=args.dias))
    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user. Partial results were saved.")
    except Exception as e:
        logger.error(f"Error running scraper: {e}")

if __name__ == "__main__":
    main()
