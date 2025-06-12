#!/usr/bin/env python3
"""
Airflow-compatible version of run_scraper.py
- Lazy imports to prevent DAG scanning timeout
- Argument parsing support
- Better error handling for Airflow context
"""

import os
import csv
import hashlib
import feedparser
import sys
import time
import random
import asyncio
import importlib.util
import argparse
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import gc
import traceback

# Configure logging for Airflow compatibility
def setup_airflow_logging():
    """Setup logging that works well with Airflow UI"""
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get logger
    logger = logging.getLogger("run_scraper_airflow")
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Add console handler for Airflow UI - redirect to stdout instead of stderr
    console_handler = logging.StreamHandler(sys.stdout)  # Use stdout instead of stderr
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Ensure logs are flushed immediately
    console_handler.flush = lambda: sys.stdout.flush()
    
    return logger

# Initialize logger
logger = setup_airflow_logging()

def log_progress(message, level="info", flush=True):
    """Log message with immediate flush for Airflow UI"""
    # For info messages, use direct print to stdout to avoid stderr completely
    if level == "info":
        print(message)
    elif level == "warning":
        logger.warning(message)
    elif level == "error":
        logger.error(message)
    elif level == "debug":
        logger.debug(message)
    
    if flush:
        # Make sure to flush stdout
        sys.stdout.flush()

def log_statistics(stats_dict, title="Statistics"):
    """Log statistics in a formatted way for better readability"""
    log_progress(f"=" * 50)
    log_progress(f"{title}")
    log_progress(f"=" * 50)
    for key, value in stats_dict.items():
        log_progress(f"  {key}: {value}")
    log_progress(f"=" * 50)

# Lazy imports to prevent DAG scanning timeout
def lazy_imports():
    """Import dependencies only when needed"""
    global UserAgent, retry, stop_after_attempt, wait_exponential, wait_random, aiohttp
    from fake_useragent import UserAgent
    from tenacity import retry, stop_after_attempt, wait_exponential, wait_random
    import aiohttp
    
    # Import local utilities
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from utils.helpers import load_keywords, carregar_paroquias_com_municipios, gerar_id
    return load_keywords, carregar_paroquias_com_municipios, gerar_id
#NOTE:
# Global variables - will be initialized in main function
GOOGLE_NEWS_TEMPLATE = "https://news.google.com/rss/search?q={query}+{municipio}+Portugal&hl=pt-PT&gl=PT&ceid=PT:pt{date_filter}"
KEYWORDS = []
LOCALIDADES = {}
MUNICIPIOS = []
DISTRITOS = []
TODAS_LOCALIDADES = []

# Balanced aggressive settings - find the sweet spot
MAX_CONCURRENT_REQUESTS = 180  # Reduced from 250 for better stability
MIN_DELAY = 0.005  # Slightly increased from 0.003
MAX_DELAY = 0.2    # Slightly increased from 0.15
BATCH_SIZE = 250   # Reduced from 350 for better stability
# NOTE:

# Initial semaphore value - will be adjusted dynamically
SEMAPHORE = None

# Dynamic concurrency control parameters - adaptive
CONCURRENCY_CONTROL = {
    'current_limit': MAX_CONCURRENT_REQUESTS,
    'min_limit': 80,   # Increased from 100 for better recovery
    'max_limit': 220,  # Reduced from 300 for stability
    'success_streak': 0,
    'failure_streak': 0,
    'adjustment_threshold': 5,  # Increased from 4 for more stability
    'last_adjustment': time.time(),
    'recovery_mode': False,     # Add recovery mode
    'last_major_failure': 0,    # Track major failures
    'stable_performance_count': 0  # Track stable periods
}

# Add new constants
USER_AGENTS = None
CACHE = {}
CACHE_DURATION = 7200  # 2 hours cache
DOMAINS_RATE_LIMITS = defaultdict(lambda: {'delay': 0.2, 'last_access': 0, 'success': 0, 'fail': 0})
RETRY_DELAY_MULTIPLIER = 1.15  # Slightly increased from 1.1
SUCCESS_THRESHOLD = 2          # Increased from 1 for more stability

# Proxy rotation
PROXY_LIST = []
USE_PROXIES = False

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

# Adaptive request patterns
REQUEST_PATTERNS = {
    "delays": {
        "between_requests": (0.02, 0.3),    # Slightly increased
        "between_batches": (0.15, 0.6),     # Slightly increased
        "error_cooldown": (0.8, 2.0),       # Slightly increased
        "captcha_cooldown": (12.0, 25.0),   # Slightly increased
        "recovery_cooldown": (3.0, 8.0)     # New: recovery mode cooldown
    },
    "batch_variation": (0.96, 1.04)  # Slightly more variation
}

# Request distribution patterns - adaptive
REQUEST_DISTRIBUTION = {
    "burst_chance": 0.45,      # Reduced from 0.6
    "burst_size": (4, 10),     # Reduced from (5, 12)
    "pause_chance": 0.05,      # Slightly increased from 0.03
    "pause_length": (1.5, 4),  # Slightly increased
    "daily_limit": 12000,      # Reduced from 15000
    "hourly_pattern": {
        "working_hours": [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        "after_hours": [20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7],
        "working_weight": 0.92,  # Slightly reduced from 0.95
        "after_weight": 0.4      # Reduced from 0.5
    }
}

# Advanced cache with memory optimization
MAX_CACHE_ITEMS = 20000
CACHE_CLEANUP_THRESHOLD = 0.20

# Memory-efficient caching system
class CacheEntry:
    __slots__ = ['timestamp', 'data']
    
    def __init__(self, timestamp, data):
        self.timestamp = timestamp
        self.data = data

def format_date_filter(date_str):
    """Format date filter for Google News URL."""
    if not date_str:
        return ""
    
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        return f"&when:1d"
    except ValueError:
        return ""

def update_concurrency_control(success, response_time):
    """Enhanced adaptive concurrency control with recovery mode"""
    control = CONCURRENCY_CONTROL
    current_time = time.time()
    
    # Adjust timing based on recovery mode
    min_adjustment_interval = 0.5 if control['recovery_mode'] else 1.5
    
    if current_time - control['last_adjustment'] < min_adjustment_interval:
        return
    
    if success:
        control['success_streak'] += 1
        control['failure_streak'] = max(0, control['failure_streak'] - 1)
        control['stable_performance_count'] += 1
        
        # Exit recovery mode after sustained success
        if control['recovery_mode'] and control['stable_performance_count'] > 15:
            control['recovery_mode'] = False
            log_progress(f"🔄 A sair do modo de recuperação - desempenho estabilizado")
        
        if control['success_streak'] >= control['adjustment_threshold']:
            # More conservative increases during recovery
            if control['recovery_mode']:
                if response_time < 0.8 and control['current_limit'] < control['max_limit']:
                    control['current_limit'] = min(control['max_limit'], 
                                                  control['current_limit'] + 3)
                    logger.debug(f"⬆️ Modo de recuperação: aumento gradual para {control['current_limit']}")
            else:
                # Normal aggressive increases
                if response_time < 0.6 and control['current_limit'] < control['max_limit']:
                    control['current_limit'] = min(control['max_limit'], 
                                                  int(control['current_limit'] * 1.15))
                    logger.debug(f"⬆️ Concorrência aumentada para {control['current_limit']}")
                elif response_time < 1.2 and control['current_limit'] < control['max_limit']:
                    control['current_limit'] = min(control['max_limit'], 
                                                  control['current_limit'] + 8)
                    logger.debug(f"⬆️ Concorrência aumentada para {control['current_limit']}")
            
            control['success_streak'] = 0
            control['last_adjustment'] = current_time
    else:
        control['failure_streak'] += 1
        control['success_streak'] = 0
        control['stable_performance_count'] = 0
        
        # Enter recovery mode on sustained failures
        if control['failure_streak'] >= 3 and not control['recovery_mode']:
            control['recovery_mode'] = True
            control['last_major_failure'] = current_time
            log_progress(f"🔄 A entrar em modo de recuperação devido a falhas consecutivas")
        
        # More aggressive reduction during failures
        if control['failure_streak'] >= 2:
            reduction_factor = 0.75 if control['recovery_mode'] else 0.85
            control['current_limit'] = max(control['min_limit'], 
                                          int(control['current_limit'] * reduction_factor))
            logger.warning(f"⬇️ Concorrência reduzida para {control['current_limit']} devido a falhas")
            control['failure_streak'] = 0
            control['last_adjustment'] = current_time
        elif response_time > 4.0:
            reduction_factor = 0.9 if control['recovery_mode'] else 0.95
            control['current_limit'] = max(control['min_limit'], 
                                          int(control['current_limit'] * reduction_factor))
            logger.debug(f"⬇️ Concorrência reduzida para {control['current_limit']} devido a respostas lentas")
            control['last_adjustment'] = current_time

def adjust_domain_delay(domain, success):
    """Enhanced domain-specific delay adjustment"""
    domain_stats = DOMAINS_RATE_LIMITS[domain]
    
    if success:
        domain_stats['success'] += 1
        domain_stats['fail'] = max(0, domain_stats['fail'] - 1)
        
        if domain_stats['success'] >= SUCCESS_THRESHOLD:
            # More conservative reduction during recovery mode
            reduction_factor = 0.9 if CONCURRENCY_CONTROL['recovery_mode'] else 0.8
            domain_stats['delay'] = max(MIN_DELAY, domain_stats['delay'] * reduction_factor)
            domain_stats['success'] = 0
    else:
        domain_stats['fail'] += 1
        domain_stats['success'] = 0
        
        if domain_stats['fail'] >= 2:
            # More aggressive penalty during failures
            penalty_multiplier = 1.3 if CONCURRENCY_CONTROL['recovery_mode'] else RETRY_DELAY_MULTIPLIER
            max_penalty = MAX_DELAY * 2 if CONCURRENCY_CONTROL['recovery_mode'] else MAX_DELAY * 1.5
            domain_stats['delay'] = min(max_penalty, domain_stats['delay'] * penalty_multiplier)
            domain_stats['fail'] = 0

async def wait_for_domain(domain):
    """Wait appropriate time for a domain - minimal delays"""
    stats = DOMAINS_RATE_LIMITS[domain]
    current_time = time.time()
    elapsed = current_time - stats['last_access']
    
    if elapsed < stats['delay']:
        wait_time = stats['delay'] - elapsed
        wait_time += random.uniform(0, wait_time * 0.05)  # Minimal jitter
        await asyncio.sleep(wait_time)
    
    # Minimal jitter
    jitter = random.uniform(-0.02, 0.02) * stats['delay']
    stats['last_access'] = time.time() + jitter

async def apply_request_distribution():
    """Enhanced request distribution with recovery mode awareness"""
    control = CONCURRENCY_CONTROL
    
    # Adjust patterns based on recovery mode
    if control['recovery_mode']:
        burst_chance = REQUEST_DISTRIBUTION["burst_chance"] * 0.5  # Reduce bursts in recovery
        pause_chance = REQUEST_DISTRIBUTION["pause_chance"] * 2    # More pauses in recovery
    else:
        burst_chance = REQUEST_DISTRIBUTION["burst_chance"]
        pause_chance = REQUEST_DISTRIBUTION["pause_chance"]
    
    if random.random() < burst_chance:
        return  # No delay for burst requests
    
    if random.random() < pause_chance:
        pause_length = REQUEST_PATTERNS["delays"]["recovery_cooldown"] if control['recovery_mode'] else REQUEST_DISTRIBUTION["pause_length"]
        pause_time = random.uniform(*pause_length)
        mode_indicator = " (recuperação)" if control['recovery_mode'] else ""
        log_progress(f"📢 Pausa ({pause_time:.1f}s){mode_indicator}")
        await asyncio.sleep(pause_time)
        return
    
    # Adaptive delay calculation
    base_delay = random.uniform(MIN_DELAY, MAX_DELAY)
    current_hour = datetime.now().hour
    
    if current_hour in REQUEST_DISTRIBUTION["hourly_pattern"]["working_hours"]:
        weight = REQUEST_DISTRIBUTION["hourly_pattern"]["working_weight"]
    else:
        weight = REQUEST_DISTRIBUTION["hourly_pattern"]["after_weight"]
    
    # Increase delays during recovery mode
    delay_multiplier = 1.5 if control['recovery_mode'] else 1.0
    adjusted_delay = base_delay * (1 + (1 - weight) * 0.3) * delay_multiplier
    
    await asyncio.sleep(adjusted_delay)

async def fetch_news(session, keyword, localidade, date_filter, dias):
    """Enhanced fetch with better error handling and recovery mode awareness"""
    await apply_request_distribution()
    
    # Adjust retry attempts based on recovery mode
    max_attempts = 1 if CONCURRENCY_CONTROL['recovery_mode'] else 2
    
    @retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=0.7, min=0.8, max=4) + wait_random(0, 0.8),
        reraise=True
    )
    async def _fetch_with_retry():
        async with SEMAPHORE:
            query_url = GOOGLE_NEWS_TEMPLATE.format(
                query=keyword.replace(" ", "+"),
                municipio=localidade.replace(" ", "+"),
                date_filter=date_filter
            )
            
            domain = COOKIE_MANAGER.get_domain_from_url(query_url)
            cookies = COOKIE_MANAGER.get_for_url(query_url)
            headers = random.choice(BROWSER_HEADERS)
            
            proxy = None
            if USE_PROXIES and PROXY_LIST:
                proxy = random.choice(PROXY_LIST)
            
            try:
                # Adaptive timeout based on recovery mode
                timeout_base = (5, 8) if CONCURRENCY_CONTROL['recovery_mode'] else (4, 7)
                timeout_value = random.uniform(*timeout_base)
                timeout = session.timeout.__class__(total=timeout_value)
                
                request_start = time.time()
                
                async with session.get(query_url, 
                                       headers=headers, 
                                       timeout=timeout, 
                                       proxy=proxy, 
                                       cookies=cookies,
                                       allow_redirects=True) as response:
                    
                    COOKIE_MANAGER.update_from_response(response)
                    response_time = time.time() - request_start
                    update_concurrency_control(response.status == 200, response_time)
                    
                    if response.status == 429:
                        logger.warning(f"Limite de taxa detetado para {domain}")
                        adjust_domain_delay(domain, False)
                        cooldown_type = "recovery_cooldown" if CONCURRENCY_CONTROL['recovery_mode'] else "error_cooldown"
                        cooldown = random.uniform(*REQUEST_PATTERNS["delays"][cooldown_type])
                        await asyncio.sleep(cooldown)
                        raise Exception("Limite de taxa excedido")
                    
                    if not response.ok:
                        adjust_domain_delay(domain, False)
                        raise Exception(f"Erro HTTP {response.status}")
                    
                    content = await response.text()
                    feed = feedparser.parse(content)
                    
                    if not feed or not hasattr(feed, 'entries') or len(feed.entries) == 0:
                        if "captcha" in content.lower():
                            adjust_domain_delay(domain, False)
                            captcha_cooldown = random.uniform(*REQUEST_PATTERNS["delays"]["captcha_cooldown"])
                            await asyncio.sleep(captcha_cooldown)
                            raise Exception("CAPTCHA detetado")
                    
                    # Simplified success logging
                    if len(feed.entries) > 0:
                        recovery_indicator = " (R)" if CONCURRENCY_CONTROL['recovery_mode'] else ""
                        log_progress(f"🔎 {keyword}/{localidade}: {len(feed.entries)}{recovery_indicator}")
                    
                    # Process articles with same speed optimizations
                    limite_dias = datetime.utcnow() - timedelta(days=dias)
                    limite_superior = datetime.utcnow()
                    
                    articles = []
                    load_keywords, carregar_paroquias_com_municipios, gerar_id = lazy_imports()
                    
                    for item in feed.entries:
                        try:
                            published_date = item.get("published", "")
                            if not published_date:
                                continue
                            
                            pub_date = None
                            try:
                                pub_date_obj = datetime.strptime(published_date.replace("GMT", "+0000"), "%a, %d %b %Y %H:%M:%S %z")
                                pub_date = pub_date_obj.replace(tzinfo=None).date()
                            except (ValueError, AttributeError):
                                try:
                                    pub_date_obj = datetime.strptime(published_date.split(" +")[0], "%a, %d %b %Y %H:%M:%S")
                                    pub_date = pub_date_obj.date()
                                except (ValueError, AttributeError):
                                    continue
                            
                            if pub_date and limite_dias.date() <= pub_date <= limite_superior.date():
                                # Ensure the article data format matches processar_relevantes_airflow expectations
                                article_data = {
                                    "ID": gerar_id(item.get("link", "")),
                                    "keyword": keyword,
                                    "localidade": localidade,
                                    "title": item.get("title", "").strip(),
                                    "link": item.get("link", ""),
                                    "published": published_date,
                                    "collection_date": datetime.now().strftime("%Y-%m-%d")
                                }
                                
                                # Validate required fields before adding
                                if all([article_data["ID"], article_data["link"], article_data["title"]]):
                                    articles.append(article_data)
                        except Exception:
                            continue
                    
                    adjust_domain_delay(domain, True)
                    return articles
                    
            except asyncio.TimeoutError:
                adjust_domain_delay(domain, False)
                await asyncio.sleep(random.uniform(0.3, 1.0))
                raise Exception("Pedido expirou")
            except Exception as e:
                adjust_domain_delay(domain, False)
                await asyncio.sleep(random.uniform(0.3, 1.0))
                raise
    
    return await _fetch_with_retry()

async def process_batch(session, batch, date_filter, dias):
    """Process batch - ultra-fast version"""
    random.shuffle(batch)
    
    # Use maximum available concurrency
    concurrency = min(len(batch), CONCURRENCY_CONTROL['current_limit'])
    chunk_size = max(5, concurrency // 2)  # Larger chunks
    chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
    
    all_results = []
    total_articles_in_batch = 0
    
    for chunk_idx, chunk in enumerate(chunks):
        chunk_semaphore = asyncio.Semaphore(min(len(chunk), concurrency))
        
        async def process_item(keyword, localidade):
            async with chunk_semaphore:
                try:
                    return await fetch_news(session, keyword, localidade, date_filter, dias)
                except Exception:
                    return []  # Simplified error handling for speed
        
        tasks = [process_item(keyword, localidade) for keyword, localidade in chunk]
        chunk_results = await asyncio.gather(*tasks, return_exceptions=False)
        
        chunk_results = [r for r in chunk_results if r]
        chunk_articles = sum(len(result) for result in chunk_results)
        total_articles_in_batch += chunk_articles
        all_results.extend(chunk_results)
        
        # Minimal inter-chunk delay
        if chunk_idx < len(chunks) - 1:
            await asyncio.sleep(random.uniform(0.01, 0.05))
    
    log_progress(f"  Lote: {total_articles_in_batch} artigos")
    return all_results

def prioritize_combinations(combinations, dias):
    """Prioritize combinations for more efficient scraping"""
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

def save_intermediate_csv(data, output_dir, date_str, output_filename=None):
    """Save scraped data to CSV files with controlled paths"""
    if not data:
        return 0
    
    # Ensure consistent field names that match processar_relevantes_airflow expectations
    fieldnames = ["ID", "keyword", "localidade", "title", "link", "published", "collection_date"]
    
    # Use controlled output paths
    if output_filename:
        primary_csv = os.path.join(output_dir, output_filename)
    else:
        primary_csv = os.path.join(output_dir, f"intermediate_google_news_{date_str}.csv")
    
    # Also create final output file
    final_csv = os.path.join(output_dir, f"google_news_articles_{date_str}.csv")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Validate data format before saving
    validated_data = []
    for article in data:
        # Ensure all required fields are present and properly formatted
        validated_article = {
            "ID": article.get("ID", ""),
            "keyword": article.get("keyword", ""),
            "localidade": article.get("localidade", ""),
            "title": article.get("title", ""),
            "link": article.get("link", ""),
            "published": article.get("published", ""),
            "collection_date": article.get("collection_date", datetime.now().strftime("%Y-%m-%d"))
        }
        
        # Only add articles with valid data
        if validated_article["ID"] and validated_article["link"] and validated_article["title"]:
            validated_data.append(validated_article)
    
    if not validated_data:
        log_progress("⚠️ Nenhum artigo válido para guardar", "warning")
        return 0
    
    # Save to primary intermediate file
    file_exists = os.path.isfile(primary_csv)
    try:
        with open(primary_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(validated_data)
        log_progress(f"✅ Guardados {len(validated_data)} artigos em {primary_csv}")
    except Exception as e:
        log_progress(f"Erro ao guardar em {primary_csv}: {e}", "error")
    
    # Also save to final file (for processing pipeline)
    try:
        with open(final_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(validated_data)
        log_progress(f"✅ Ficheiro final criado: {final_csv}")
    except Exception as e:
        log_progress(f"Erro ao criar ficheiro final {final_csv}: {e}", "error")
    
    return len(validated_data)

async def check_google_news_access(session):
    """Check if we have access to Google News RSS feed before starting the scraping task"""
    log_progress("🔍 A verificar acesso ao feed RSS do Google News...")
    log_progress(f"🔍 Detalhes da sessão: {type(session)}", "debug")
    
    # Simple test query to Google News
    test_url = "https://news.google.com/rss/search?q=test+Portugal&hl=pt-PT&gl=PT&ceid=PT:pt"
    log_progress(f"🔍 URL de teste: {test_url}", "debug")
    
    try:
        # Create randomized headers for the test request
        headers = random.choice(BROWSER_HEADERS)
        log_progress(f"🔍 A usar cabeçalhos: {headers}", "debug")
        
        # Set a short timeout for quick feedback
        timeout = aiohttp.ClientTimeout(total=10)  # 10 seconds timeout for test request
        log_progress(f"🔍 Timeout definido para: {timeout}", "debug")
        
        log_progress("🔍 A enviar pedido de teste ao Google News...", "debug")
        async with session.get(test_url, headers=headers, timeout=timeout) as response:
            log_progress(f"🔍 Estado da resposta: {response.status}", "debug")
            
            if response.status != 200:
                log_progress(f"❌ Não foi possível aceder ao Google News: estado HTTP {response.status}", "error")
                return False
            
            log_progress("🔍 A ler conteúdo da resposta...", "debug")
            content = await response.text()
            content_preview = content[:200] + "..." if len(content) > 200 else content
            log_progress(f"🔍 Pré-visualização do conteúdo: {content_preview}", "debug")
            
            # Check if the response looks like an RSS feed
            if "<rss" not in content or "<channel>" not in content:
                log_progress("❌ Acesso ao Google News bloqueado ou a devolver conteúdo inválido", "error")
                return False
            
            # Check if we're being blocked or getting a CAPTCHA
            if "captcha" in content.lower():
                log_progress("❌ Google News está a mostrar um CAPTCHA - acesso bloqueado", "error")
                return False
            
            # Check if feed contains entries
            if "<item>" not in content:
                log_progress("⚠️ Google News acessível mas não devolveu itens", "warning")
                # Still return True since we have access, just no items for this query
            
            log_progress("✅ Feed RSS do Google News está acessível", "info")
            return True
            
    except asyncio.TimeoutError:
        log_progress("❌ Timeout ao conectar ao feed RSS do Google News", "error")
        return False
    except Exception as e:
        log_exception(e, "Erro ao verificar acesso ao Google News")
        return False

async def create_randomized_browser_fingerprint():
    """Create a more realistic browser fingerprint with randomized properties"""
    base_headers = random.choice(BROWSER_HEADERS).copy()
    
    # Add realistic browser-specific headers with variation
    if "Chrome" in base_headers["User-Agent"]:
        base_headers["sec-ch-ua"] = f'"Google Chrome";v="{random.randint(90, 120)}", "Chromium";v="{random.randint(90, 120)}"'
        base_headers["sec-ch-ua-mobile"] = "?0"
        base_headers["sec-ch-ua-platform"] = random.choice(['"Windows"', '"macOS"', '"Linux"'])
    
    # Add random accept-encoding variations
    encodings = ["gzip", "deflate", "br"]
    random.shuffle(encodings)
    base_headers["Accept-Encoding"] = ", ".join(encodings[:random.randint(1, 3)])
    
    # Random connection value
    if random.random() > 0.5:
        base_headers["Connection"] = random.choice(["keep-alive", "close"])
    
    # Random viewport and screen size (realistic values)
    viewports = [(1920, 1080), (1440, 900), (1366, 768), (2560, 1440), (1280, 720)]
    viewport = random.choice(viewports)
    
    # Add these as custom headers to simulate JS fingerprinting values
    if random.random() > 0.7:
        base_headers["X-Viewport-Width"] = str(viewport[0])
        base_headers["X-Viewport-Height"] = str(viewport[1])
        base_headers["X-Device-Pixel-Ratio"] = str(random.choice([1, 1.5, 2, 2.5]))
    
    return base_headers

async def human_like_delay():
    """Ultra-minimal delays"""
    delay_type = random.choice(["quick", "normal"])
    
    if delay_type == "quick":
        delay = random.uniform(0.01, 0.05)
    else:
        delay = random.uniform(0.05, 0.15)
    
    await asyncio.sleep(delay)

async def fetch_with_playwright(url):
    """Use Playwright for harder cases"""
    from playwright.async_api import async_playwright
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context(
            user_agent=random.choice(BROWSER_HEADERS)["User-Agent"],
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        await page.goto(url)
        content = await page.content()
        await browser.close()
        return content

# Then define main functions that use these helpers
async def run_scraper_async(specific_date=None, dias=1, output_dir=None, date_str=None):
    """Main async function to run the scraper with controlled output paths"""
    start_time = time.time()
    
    log_progress("🚀 A iniciar o scraper do Google News...")
    log_progress(f"📅 Data: {specific_date or 'atual'}, Dias anteriores: {dias}")
    
    if output_dir:
        log_progress(f"📁 Diretoria de saída controlada: {output_dir}")
    if date_str:
        log_progress(f"📅 String de data: {date_str}")
    
    # Lazy import dependencies
    import aiohttp
    
    # Initialize globals with lazy imports
    global KEYWORDS, LOCALIDADES, MUNICIPIOS, DISTRITOS, TODAS_LOCALIDADES, USER_AGENTS, SEMAPHORE
    
    # Set up paths first for better Airflow compatibility
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # In Docker, we need to go up to the project root differently
    # /opt/airflow/scripts/google_scraper/scraping -> /opt/airflow
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
    
    log_progress(f"📁 Diretoria do script: {script_dir}")
    log_progress(f"📁 Raiz do projeto: {project_root}")
    
    # Load configuration with proper error handling
    try:
        log_progress("🔧 A carregar ficheiros de configuração...")
        load_keywords, carregar_paroquias_com_municipios, gerar_id = lazy_imports()
        
        # Try multiple paths for config files - more comprehensive search
        config_paths = [
            os.path.join(project_root, "config"),
            os.path.join(project_root, "simprede-airflow", "config"),
            os.path.join(script_dir, "..", "..", "..", "config"),
            os.path.join(script_dir, "..", "config"),
            os.path.join(script_dir, "config"),
            "/opt/airflow/config",
            "/opt/airflow/simprede-airflow/config",
            "config"
        ]
        
        log_progress(f"🔍 A procurar ficheiros de configuração em: {config_paths}")
        
        keywords_loaded = False
        locations_loaded = False
        
        for config_path in config_paths:
            keywords_file = os.path.join(config_path, "keywords.json")
            locations_file = os.path.join(config_path, "municipios_por_distrito.json")
            
            if os.path.exists(keywords_file) and not keywords_loaded:
                log_progress(f"📁 A carregar palavras-chave de: {keywords_file}")
                KEYWORDS = load_keywords(keywords_file, idioma="portuguese")
                keywords_loaded = True
                log_progress(f"✅ Carregadas {len(KEYWORDS)} palavras-chave")
                
            if os.path.exists(locations_file) and not locations_loaded:
                log_progress(f"📁 A carregar localizações de: {locations_file}")
                LOCALIDADES, MUNICIPIOS, DISTRITOS = carregar_paroquias_com_municipios(locations_file)
                locations_loaded = True
                log_progress(f"✅ Carregadas {len(LOCALIDADES)} freguesias, {len(MUNICIPIOS)} municípios, {len(DISTRITOS)} distritos")
                
            if keywords_loaded and locations_loaded:
                break
        
        if not keywords_loaded:
            log_progress("❌ Não foi possível carregar o ficheiro keywords.json", "error")
            log_progress(f"🔍 Procurado em: {config_paths}", "error")
            raise FileNotFoundError("keywords.json não encontrado")
            
        if not locations_loaded:
            log_progress("❌ Não foi possível carregar o ficheiro municipios_por_distrito.json", "error")
            log_progress(f"🔍 Procurado em: {config_paths}", "error")
            raise FileNotFoundError("municipios_por_distrito.json não encontrado")
            
        TODAS_LOCALIDADES = list(LOCALIDADES.keys()) + MUNICIPIOS + DISTRITOS
        
        config_stats = {
            "Palavras-chave": len(KEYWORDS),
            "Total de Localizações": len(TODAS_LOCALIDADES),
            "Freguesias": len(LOCALIDADES),
            "Municípios": len(MUNICIPIOS),
            "Distritos": len(DISTRITOS)
        }
        log_statistics(config_stats, "Configuração Carregada")
        
    except Exception as e:
        log_progress(f"❌ Erro ao carregar configuração: {e}", "error")
        raise
    
    from fake_useragent import UserAgent
    USER_AGENTS = UserAgent(browsers=['chrome', 'firefox', 'safari', 'edge'])
    
    # Initialize semaphore
    SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    # Clear cache at start
    CACHE.clear()
    log_progress("🧹 Cache limpa")
    
    # Get appropriate date
    if specific_date:
        try:
            dt = datetime.strptime(specific_date, "%Y-%m-%d")
            log_progress(f"📅 A usar data específica: {specific_date}")
        except ValueError as e:
            log_progress(f"Formato de data inválido: {e}", "error")
            raise ValueError(f"Formato de data inválido: {e}")
    else:
        dt = datetime.now() - timedelta(days=dias)
        log_progress(f"📅 A usar data calculada: {dt.strftime('%Y-%m-%d')} ({dias} dias atrás)")
    
    current_date = dt.strftime("%Y%m%d")
    current_year = dt.strftime("%Y")
    current_month = dt.strftime("%m")
    current_day = dt.strftime("%d")
    
    # Use the same path structure as processar_relevantes_airflow expects
    raw_data_dir = os.path.join(project_root, "data", "raw", current_year, current_month, current_day)
    os.makedirs(raw_data_dir, exist_ok=True)
    log_progress(f"📁 Diretoria de saída: {raw_data_dir}")

    date_filter = format_date_filter(specific_date) if specific_date else ""
    
    # Generate all combinations
    log_progress("🔄 A gerar combinações palavra-chave-localização...")
    all_combinations = [(kw, loc) for loc in TODAS_LOCALIDADES for kw in KEYWORDS]
    
    # Apply intelligent prioritization and filtering
    all_combinations = prioritize_combinations(all_combinations, dias)
    
    # Calculate an optimized batch size
    total_combinations = len(all_combinations)
    
    combination_stats = {
        "Total de Combinações": total_combinations,
        "Palavras-chave": len(KEYWORDS),
        "Localizações": len(TODAS_LOCALIDADES),
        "Palavras-chave Prioritárias": len(PRIORITY_KEYWORDS)
    }
    log_statistics(combination_stats, "Combinações Geradas")
    
    if total_combinations == 0:
        log_progress("❌ Nenhuma combinação palavra-chave-localização foi gerada!", "error")
        return 0
    
    # Balanced batch sizing - optimize for stability
    if total_combinations > 5000:
        adjusted_batch_size = min(BATCH_SIZE * 2, max(150, int(BATCH_SIZE * 1.3)))
    elif total_combinations > 2000:
        adjusted_batch_size = min(BATCH_SIZE * 1.5, max(180, int(BATCH_SIZE * 1.2)))
    elif total_combinations > 1000:
        adjusted_batch_size = min(BATCH_SIZE, max(200, int(BATCH_SIZE * 1.1)))
    else:
        adjusted_batch_size = BATCH_SIZE
    
    processing_stats = {
        "Total de Combinações": total_combinations,
        "Tamanho do Lote": adjusted_batch_size,
        "Lotes Estimados": (total_combinations + adjusted_batch_size - 1) // adjusted_batch_size,
        "Máx. Pedidos Concorrentes": CONCURRENCY_CONTROL['current_limit']
    }
    log_statistics(processing_stats, "Configuração de Processamento")
    
    # Configure session with optimized connection pooling - more aggressive
    log_progress("🔗 A configurar sessão HTTP equilibrada...")
    tcp_connector = aiohttp.TCPConnector(
        limit=CONCURRENCY_CONTROL['max_limit'] * 4,  # Reduced from 6
        ttl_dns_cache=2400,  # Reduced from 3600
        enable_cleanup_closed=True,
        force_close=False,
        ssl=False,
        limit_per_host=random.randint(30, 45)  # Reduced from 40-60
    )
    
    connect_timeout = random.uniform(2.5, 3.5)      # Slightly increased
    sock_read_timeout = random.uniform(5, 7)        # Slightly increased
    total_timeout = connect_timeout + sock_read_timeout + random.uniform(0.5, 1.5)  # Total: ~8-12 seconds
    
    timeout = aiohttp.ClientTimeout(
        total=total_timeout, 
        connect=connect_timeout,
        sock_read=sock_read_timeout
    )
    
    total_articles_collected = 0
    
    async with aiohttp.ClientSession(connector=tcp_connector, timeout=timeout) as session:
        # Check access to Google News before starting
        has_access = await check_google_news_access(session)
        
        if not has_access:
            log_progress("🛑 Não é possível continuar com o scraping pois o Google News não está acessível", "error")
            return 0
        
        log_progress("🚀 A iniciar processamento por lotes...")
        
        # Process in dynamic batches
        for i in range(0, total_combinations, adjusted_batch_size):
            variation = random.uniform(*REQUEST_PATTERNS["batch_variation"])
            current_batch_size = min(
                int(adjusted_batch_size * variation),
                total_combinations - i
            )
            
            batch = all_combinations[i:i + current_batch_size]
            batch_number = i // adjusted_batch_size + 1
            total_batches = (total_combinations + adjusted_batch_size - 1) // adjusted_batch_size
            
            percent_complete = (i / total_combinations) * 100
            elapsed_time = time.time() - start_time
            
            log_progress("=" * 60)
            log_progress(f"📦 LOTE {batch_number}/{total_batches} ({percent_complete:.1f}% completo)")
            log_progress(f"⏱️  Tempo decorrido: {elapsed_time:.1f}s")
            log_progress(f"📊 Combinações no lote: {current_batch_size}")
            log_progress("=" * 60)
            
            batch_start_time = time.time()
            results = await process_batch(session, batch, date_filter, dias)
            batch_processing_time = time.time() - batch_start_time
            
            # Flatten results and save
            articles = [article for result in results for article in result]
            batch_articles = len(articles)
            total_articles_collected += batch_articles
            
            if articles:
                save_intermediate_csv(articles, raw_data_dir, current_date)
                log_progress(f"✅ Guardados {batch_articles} artigos do lote {batch_number}")
            else:
                log_progress(f"⚠️  Nenhum artigo encontrado no lote {batch_number}", "warning")
            
            # Batch statistics
            batch_stats = {
                "Artigos Encontrados": batch_articles,
                "Tempo de Processamento": f"{batch_processing_time:.1f}s",
                "Total de Artigos Até Agora": total_articles_collected,
                "Média por Combinação": f"{batch_articles/current_batch_size:.2f}" if current_batch_size > 0 else "0"
            }
            log_statistics(batch_stats, f"Resultados do Lote {batch_number}")
            
            # Adaptive delay between batches
            if i + current_batch_size < total_combinations:
                batch_time = time.time() - batch_start_time
                
                # Recovery mode gets longer delays
                if CONCURRENCY_CONTROL['recovery_mode']:
                    delay = random.uniform(1.0, 2.5)
                else:
                    # Normal adaptive delays
                    if batch_time < 3:
                        delay = random.uniform(0.3, 0.8)
                    elif batch_time < 8:
                        delay = random.uniform(0.2, 0.6)
                    else:
                        delay = random.uniform(0.1, 0.4)
                
                remaining_batches = total_batches - batch_number
                est_time_remaining = int(remaining_batches * (batch_time + delay))
                
                mode_indicator = " (recuperação)" if CONCURRENCY_CONTROL['recovery_mode'] else ""
                log_progress(f"⏳ {delay:.1f}s{mode_indicator} | Tempo restante estimado: {est_time_remaining//60}m{est_time_remaining%60}s")
                await asyncio.sleep(delay)
    
    # Clean up and report completion
    total_time = time.time() - start_time
    intermediate_csv = os.path.join(raw_data_dir, f"intermediate_google_news_{current_date}.csv")
    
    final_stats = {
        "Tempo Total de Processamento": f"{total_time:.1f}s ({total_time/60:.1f}m)",
        "Total de Combinações Processadas": total_combinations,
        "Total de Artigos Recolhidos": total_articles_collected,
        "Média de Artigos por Combinação": f"{total_articles_collected/total_combinations:.3f}" if total_combinations > 0 else "0",
        "Taxa de Processamento": f"{total_combinations/total_time:.1f} combinações/seg" if total_time > 0 else "N/A",
        "Entradas de Cache": len(CACHE),
        "Ficheiro de Saída": intermediate_csv
    }
    log_statistics(final_stats, "RESULTADOS FINAIS")
    
    # Memory-optimized cache cleanup
    gc_before = len(CACHE)
    target_size = 1500
    if len(CACHE) > target_size:
        cache_keys = list(CACHE.keys())
        cache_keys.sort(key=lambda k: CACHE[k].timestamp, reverse=True)
        keep_keys = set(cache_keys[:target_size])
        for k in list(CACHE.keys()):
            if k not in keep_keys:
                CACHE.pop(k, None)
    log_progress(f"🧹 Otimização de cache: {gc_before} → {len(CACHE)} entradas")
    
    # Return number of articles for Airflow compatibility
    if os.path.exists(intermediate_csv):
        try:
            with open(intermediate_csv, 'r', encoding='utf-8') as f:
                article_count = sum(1 for line in f) - 1  # Subtract header
            log_progress(f"✅ Scraping concluído com sucesso! {article_count} artigos guardados.")
            return article_count
        except Exception as e:
            log_progress(f"Erro ao ler ficheiro final: {e}", "error")
            return total_articles_collected
    
    log_progress(f"✅ Scraping concluído! {total_articles_collected} artigos recolhidos.")
    return total_articles_collected

async def airflow_optimized_main(days_back=1, search_date=None, max_execution_time=3600, output_dir=None, date_str=None):
    """
    Async function for direct import by Airflow DAG with controlled output paths
    Returns number of articles scraped
    """
    log_progress("🚀 A iniciar função do scraper otimizada para Airflow")
    log_progress(f"⚙️  Parâmetros: days_back={days_back}, search_date={search_date}, max_time={max_execution_time}s")
    
    if output_dir:
        log_progress(f"📁 Diretoria de saída: {output_dir}")
    if date_str:
        log_progress(f"📅 String de data: {date_str}")
    
    try:
        # Run with timeout and controlled paths
        result = await asyncio.wait_for(
            run_scraper_async(
                specific_date=search_date, 
                dias=days_back,
                output_dir=output_dir,
                date_str=date_str
            ),
            timeout=max_execution_time
        )
        log_progress(f"🎉 Execução do Airflow concluída com sucesso! Artigos: {result}")
        return result
    except asyncio.TimeoutError:
        log_progress(f"⏰ Scraper expirou após {max_execution_time} segundos", "error")
        raise Exception(f"Execução do scraper excedeu o limite máximo de tempo de {max_execution_time} segundos")

# Cookie management
class CookieManager:
    """Manage cookies for different domains to maintain sessions"""
    def __init__(self):
        self.domain_cookies = defaultdict(dict)
        self.last_used = {}
    
    def get_domain_from_url(self, url):
        """Extract domain from URL"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc
    
    def get_for_url(self, url):
        """Get cookies appropriate for the given URL"""
        domain = self.get_domain_from_url(url)
        self.last_used[domain] = time.time()
        return self.domain_cookies.get(domain, {})
    
    def update_from_response(self, response):
        """Update cookie store with cookies from response"""
        if not hasattr(response, 'cookies') or not response.cookies:
            return
            
        domain = self.get_domain_from_url(str(response.url))
        
        # Update existing cookies
        for name, value in response.cookies.items():
            self.domain_cookies[domain][name] = value
    
    def clear_old_cookies(self, max_age=3600):
        """Clear cookies older than max_age seconds"""
        current_time = time.time()
        domains_to_clear = []
        
        for domain, last_time in self.last_used.items():
            if current_time - last_time > max_age:
                domains_to_clear.append(domain)
                
        for domain in domains_to_clear:
            self.domain_cookies.pop(domain, None)
            self.last_used.pop(domain, None)

# Initialize the cookie manager
COOKIE_MANAGER = CookieManager()

def log_exception(e, context=""):
    """Log detailed exception with traceback"""
    error_type = type(e).__name__
    error_msg = str(e)
    
    print(f"❌ DETALHES DA EXCEÇÃO {'='*30}")
    print(f"❌ Contexto: {context}")
    print(f"❌ Tipo: {error_type}")
    print(f"❌ Mensagem: {error_msg}")
    
    # Get and log the traceback
    tb_lines = traceback.format_exc().splitlines()
    print(f"❌ Rastreamento:")
    for line in tb_lines:
        print(f"❌   {line}")
    
    print(f"❌ {'='*50}")
    sys.stdout.flush()  # Force flush

# Make sure print statements are unbuffered
import functools
original_print = print
print = functools.partial(print, flush=True)

def main():
    """Main function with controlled output paths support"""
    parser = argparse.ArgumentParser(description="Executar scraper do Google News (versão Airflow)")
    parser.add_argument("--dias", type=int, default=1, help="Número de dias anteriores para fazer scraping")
    parser.add_argument("--date", type=str, help="Data específica para fazer scraping (AAAA-MM-DD)")
    parser.add_argument("--max_time", type=int, default=3600, help="Tempo máximo de execução em segundos")
    parser.add_argument("--output_dir", type=str, help="Diretoria de saída para os ficheiros")
    parser.add_argument("--date_str", type=str, help="String de data para nomes de ficheiros")
    parser.add_argument("--quiet", action="store_true", help="Executar com saída mínima")
    parser.add_argument("--debug", action="store_true", help="Ativar logging de debug")
    
    args = parser.parse_args()
    
    # Configure logging level based on flags
    if args.debug:
        logger.setLevel(logging.DEBUG)
        log_progress("🔍 Logging de DEBUG ativado", "debug")
    elif args.quiet:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)
    
    log_progress("A iniciar scraper do Google News (versão Airflow)")
    log_progress(f"Parâmetros: dias={args.dias}, date={args.date}, max_time={args.max_time}")
    log_progress(f"Paths: output_dir={args.output_dir}, date_str={args.date_str}")

    # Get appropriate date for output path
    if args.date:
        try:
            dt = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            dt = datetime.now() - timedelta(days=args.dias)
    else:
        dt = datetime.now() - timedelta(days=args.dias)
    
    current_date = dt.strftime("%Y%m%d")
    current_year = dt.strftime("%Y")
    current_month = dt.strftime("%m")
    current_day = dt.strftime("%d")
    
    # Use controlled output path if provided
    if args.output_dir and args.date_str:
        output_file = os.path.join(args.output_dir, f"intermediate_google_news_{args.date_str}.csv")
        log_progress(f"📁 A usar ficheiro de saída controlado: {output_file}")
    else:
        # Fallback to original path logic
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
        output_file = os.path.join(project_root, "data", "raw", current_year, current_month, current_day, f"intermediate_google_news_{current_date}.csv")
        log_progress(f"📁 A usar ficheiro de saída padrão: {output_file}")

    log_progress(f"⏱️ Tempo máximo de execução: {args.max_time} segundos")

    # Run the async function
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        result = loop.run_until_complete(
            airflow_optimized_main(
                days_back=args.dias,
                search_date=args.date,
                max_execution_time=args.max_time,
                output_dir=args.output_dir,
                date_str=args.date_str
            )
        )
        
        log_progress(f"✅ Scraping concluído. Guardados {result} artigos")
        return 0
        
    except KeyboardInterrupt:
        log_progress("Scraper interrompido pelo utilizador. Resultados parciais foram guardados.", "warning")
        return 1
    except Exception as e:
        log_progress(f"❌ Scraper falhou: {e}", "error")
        return 1
    finally:
        loop.close()

if __name__ == "__main__":
    sys.exit(main())

