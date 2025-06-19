# File: airflow-GOOGLE-NEWS-SCRAPER/scripts/google_scraper/tests/test_google_news_access.py
# Script para testar o acesso ao RSS do Google News e verificar se o serviço está a funcionar sem bloqueios

import requests
import feedparser
import time
import random
from fake_useragent import UserAgent
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# List of test URLs
TEST_URLS = [
    "https://news.google.com/rss/search?q=portugal+weather&hl=pt-PT&gl=PT&ceid=PT:pt",
    "https://news.google.com/rss/headlines/section/topic/WORLD?hl=pt-PT&gl=PT&ceid=PT:pt",
    "https://news.google.com/rss/search?q=portugal&hl=pt-PT&gl=PT&ceid=PT:pt"
]

def get_random_user_agent():
    try:
        ua = UserAgent()
        return ua.random
    except Exception as e:
        logger.warning(f"Could not generate random user agent: {e}")
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

def test_google_news_access(max_retries=3, delay_between_retries=5):
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
        'Accept-Language': 'pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive'
    }

    for url in TEST_URLS:
        logger.info(f"\nTesting URL: {url}")
        
        for attempt in range(max_retries):
            try:
                # Add random delay between requests
                time.sleep(random.uniform(2, 5))
                
                logger.info(f"Attempt {attempt + 1}/{max_retries}")
                response = requests.get(url, headers=headers, timeout=10)
                
                logger.info(f"HTTP Status Code: {response.status_code}")
                
                if response.status_code == 200:
                    logger.info("✅ Basic HTTP access successful")
                    
                    feed = feedparser.parse(url)
                    if feed.bozo == 0:
                        logger.info("✅ RSS feed successfully parsed")
                        logger.info(f"Found {len(feed.entries)} entries")
                        
                        if len(feed.entries) > 0:
                            logger.info("\nExample entry:")
                            logger.info(f"Title: {feed.entries[0].title}")
                            logger.info(f"Link: {feed.entries[0].link}")
                        break
                    else:
                        logger.error(f"❌ Failed to parse RSS feed: {feed.bozo_exception}")
                        
                elif response.status_code == 503:
                    logger.error("❌ Service Unavailable (503) - You are likely being rate limited")
                    
                else:
                    logger.error(f"❌ Failed to access Google News: {response.status_code}")
                
            except Exception as e:
                logger.error(f"❌ Error occurred: {str(e)}")
            
            if attempt < max_retries - 1:
                wait_time = delay_between_retries * (attempt + 1)
                logger.info(f"Waiting {wait_time} seconds before next attempt...")
                time.sleep(wait_time)

if __name__ == "__main__":
    print("Testing Google News RSS access...")
    test_google_news_access()
