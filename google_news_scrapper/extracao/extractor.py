import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from newspaper import Article, Config
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import csv
from newspaper import Article
import re
import unicodedata

def normalize(text):
    """Normalize text by removing accents, converting to lowercase and removing special characters."""
    if not text:
        return ""
    # Convert to lowercase and normalize unicode
    text = text.lower()
    # Remove accents
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    # Remove special characters but keep spaces
    return text

def word_to_number(text):
    """Convert Portuguese word numbers to digits."""
    pt_number_words = {
        "um": 1, "uma": 1, "dois": 2, "duas": 2, "três": 3, "tres": 3,
        "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8, "nove": 9,
        "dez": 10, "onze": 11, "doze": 12, "treze": 13, "catorze": 14, "quatorze": 14,
        "quinze": 15, "dezasseis": 16, "dezesseis": 16, "dezessete": 17,
        "dezoito": 18, "dezanove": 19, "dezenove": 19, "vinte": 20,
        "trinta": 30, "quarenta": 40, "cinquenta": 50, "sessenta": 60,
        "setenta": 70, "oitenta": 80, "noventa": 90, "cem": 100
    }
    
    # Normalize text
    text = normalize(text)
    
    # Check direct number words
    words = text.lower().split()
    for word in words:
        if word in pt_number_words:
            return pt_number_words[word]
    
    return None

 #TEST:
def resolve_with_newspaper(url):
    try:
        if "news.google.com" in url and "/articles/" not in url:
            print(f"⚠️ URL inválido para newspaper: {url}")
            return None

        article = Article(url)
        article.download()
        return article.source_url or article.canonical_link or article.url
    except Exception as e:
        print(f"⚠️ Erro a resolver com newspaper3k: {e}")
        return None


#TEST:
def resolve_google_news_url(url, driver_path="/opt/homebrew/bin/chromedriver", max_wait_time=10):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    try:
        print(f"🌐 Acessando o link: {url}")
        driver.get(url)
        wait = WebDriverWait(driver, max_wait_time)

        # Aceitar consentimento se necessário
        if "consent.google.com" in driver.current_url:
            print("⚠️ Página de consentimento detectada. Tentando aceitar...")
            try:
                # Try multiple approaches to accept consent
                # First approach - look for Accept all button in English
                try:
                    accept_all_button = wait.until(
                        EC.element_to_be_clickable((By.XPATH, '//button[.//span[contains(text(), "Accept all") or contains(text(), "Aceitar tudo") or contains(text(), "I agree")]]'))
                    )
                    accept_all_button.click()
                    print("✅ Consentimento aceite via botão Accept all!")
                except Exception:
                    # Second approach - try to find the form and use the first button (usually Accept)
                    try:
                        form = wait.until(EC.presence_of_element_located((By.TAG_NAME, "form")))
                        buttons = form.find_elements(By.TAG_NAME, "button")
                        if buttons:
                            buttons[0].click()
                            print("✅ Consentimento aceite via primeiro botão do formulário!")
                    except Exception:
                        # Third approach - try to use iframe if exists
                        try:
                            iframe = wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
                            driver.switch_to.frame(iframe)
                            accept_btn = driver.find_element(By.XPATH, '//button[contains(@jsname, "higCR")]')
                            accept_btn.click()
                            driver.switch_to.default_content()
                            print("✅ Consentimento aceite via iframe!")
                        except Exception as e:
                            print(f"⚠️ Não foi possível aceitar o consentimento via métodos alternativos: {e}")
            except Exception as e:
                print(f"❌ Não foi possível aceitar o consentimento: {e}")

        # Espera redireção
        wait.until(lambda d: not d.current_url.startswith("https://news.google.com/")
                            and not d.current_url.startswith("https://consent.google.com/"))

        final_url = driver.current_url
        print(f"✅ URL final resolvido: {final_url}")
        return final_url

    except Exception as e:
        print(f"❌ Erro ao resolver URL do Google News: {e}")
        return None
    finally:
        driver.quit()



def get_original_url_via_requests(google_rss_url):
    """Retrieve the original article URL using Google's internal API."""
    try:
        print("🔄 Tentando obter o URL original via requests...")
        guid = urlparse(google_rss_url).path.replace('/rss/articles/', '')

        param = '["garturlreq",[["en-US","US",["FINANCE_TOP_INDICES","WEB_TEST_1_0_0"],null,null,1,1,"US:en",null,null,null,null,null,null,null,0,5],"en-US","US",true,[2,4,8],1,true,"661099999",0,0,null,0],{guid}]'
        payload = urlencode({
            'f.req': [[['Fbv4je', param.format(guid=guid), 'null', 'generic']]]
        })

        headers = {
            'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }

        url = "https://news.google.com/_/DotsSplashUi/data/batchexecute"
        response = requests.post(url, headers=headers, data=payload)

        if response.status_code == 200:
            array_string = json.loads(response.text.replace(")]}'", ""))[0][2]
            article_url = json.loads(array_string)[1]
            print(f"✅ URL original obtido via requests: {article_url}")
            return article_url
        else:
            print(f"❌ Falha ao obter URL via requests. Código de status: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Erro ao usar requests para obter o URL original: {e}")
        return None

def fetch_and_extract_article_text(url: str) -> str:
    """
    Fetches the content of a webpage and extracts the main article text with improved accuracy.
    """
    if not url.startswith("http"):
        print(f"⚠️ URL inválido: {url}")
        return ""

    try:
        print(f"🌐 Fetching URL: {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive"
        }
        
        # Add timeout and retry mechanism
        session = requests.Session()
        retries = 3
        
        for attempt in range(retries):
            try:
                response = session.get(url, timeout=15, headers=headers)
                response.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt < retries - 1:
                    print(f"⚠️ Tentativa {attempt+1} falhou: {str(e)}. Tentando novamente...")
                    continue
                else:
                    print(f"❌ Todas as tentativas falharam para {url}: {str(e)}")
                    return ""
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Remove unwanted elements more thoroughly
        for element in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'iframe', 
                                     'aside', 'noscript', 'meta', 'head', 'svg', 'button']):
            element.decompose()
            
        # Remove elements with specific classes/IDs often used for ads, comments, etc.
        for selector in ['.ad', '.ads', '.advertisement', '#comments', '.comments', 
                        '.social', '.share', '.related', '.sidebar', '.promoted', '.gdpr']:
            for element in soup.select(selector):
                element.decompose()

        # Try multiple content selectors - prioritize more specific ones first
        article_selectors = [
            'article', 
            'main',
            '[role="main"]',
            '.article-body',
            '.post-content',
            '.entry-content',
            '.story-body',
            '#article-body',
            '.news-article',
            '.content-body',
            '.story-content',
            '.article-content',
            '.news-content',
            '.article__body',
            '.article_body',
            '.article-text',
            '.article__content',
            '.post__content',
            '.main-content',
            '.container', # Less specific as fallback
            '#content',
            '.content'
        ]

        # First try: look for article content in main containers
        for selector in article_selectors:
            content = soup.select_one(selector)
            if content:
                # Extract paragraphs from content, with minimum length requirement
                paragraphs = content.find_all('p')
                if paragraphs:
                    # Filter paragraphs by length (at least 30 chars) to avoid menu items, captions, etc.
                    valid_paragraphs = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) >= 30]
                    if valid_paragraphs:
                        return ' '.join(valid_paragraphs)

        # Second try: find the div with the most paragraph content
        divs = soup.find_all('div')
        best_div = None
        max_text_length = 0
        
        for div in divs:
            paragraphs = div.find_all('p')
            if paragraphs:
                text_length = sum(len(p.get_text(strip=True)) for p in paragraphs)
                if text_length > max_text_length:
                    max_text_length = text_length
                    best_div = div
        
        if best_div:
            paragraphs = best_div.find_all('p')
            valid_paragraphs = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) >= 30]
            if valid_paragraphs:
                return ' '.join(valid_paragraphs)

        # Fallback: try to find article by looking for clusters of paragraphs
        paragraphs = soup.find_all('p')
        if paragraphs:
            valid_paragraphs = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) >= 30]
            if valid_paragraphs:
                return ' '.join(valid_paragraphs)

        print(f"⚠️ No article content found for URL: {url}")
        return ""

    except Exception as e:
        print(f"❌ Error extracting content: {str(e)}")
        return ""

def fetch_and_extract_article_text_dynamic(url: str) -> str:
    """
    Fetches the content of a webpage using Selenium and extracts the main article text.
    """
    if "news.google.com" in url:
        print(f"⚠️ Ignorado URL do Google News (redirecionamento): {url}")
        return ""

    try:
        print(f"🌐 Fetching URL dynamically: {url}")
        options = Options()
        options.add_argument("--headless")  # Run in headless mode
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        service = Service("/path/to/chromedriver")  # Update with the path to your ChromeDriver
        driver = webdriver.Chrome(service=service, options=options)

        driver.get(url)

        # Remove pop-ups and overlays
        driver.execute_script("""
            let modals = document.querySelectorAll('[class*="popup"], [class*="modal"], [id*="popup"]');
            modals.forEach(el => el.remove());
        """)

        # Wait for the main content to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article-body, .content"))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()

        # Attempt to extract the main article text
        containers = [
            soup.find('article'),
            soup.find('main'),
            soup.find('div', class_='article-body'),
            soup.find('div', class_="story"),  # Fixed syntax error
            soup.find('div', 'content'),
            soup.find('body')
        ]
        for container in containers:
            if container:
                paragraphs = container.find_all('p')
                text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
                if text:
                    print(f"🔍 Extracted text from container: {container.name}")
                    return " ".join(text).strip()

        # Fallback: Try extracting all <p> tags if no container is found
        paragraphs = soup.find_all('p')
        text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
        if text:
            print(f"🔍 Extracted text from fallback <p> tags.")
            return " ".join(text).strip()

        print(f"⚠️ No article or content found for URL: {url}")
        return ""

    except Exception as e:
        print(f"⚠️ Error fetching URL dynamically {url}: {e}")
        return ""

def extract_article_text(soup):
    if soup is None:
        return ""

    containers = [
        soup.find('article'),
        soup.find('main'),
        soup.find('div', class_='article-body'),
        soup.find('div', class_='story'),
        soup.find('body')
    ]
    for container in containers:
        if container:
            paragraphs = container.find_all('p')
            texto = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
            if texto:
                return " ".join(texto).strip()
    # fallback
    paragraphs = soup.find_all('p')
    texto = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
    return " ".join(texto).strip()


def extract_victim_counts(text):
    """
    Extracts victim counts from article text with improved precision.
    Returns a dictionary with counts for different victim types.
    """
    if not text:
        return {"fatalities": 0, "injured": 0, "evacuated": 0, "displaced": 0, "missing": 0}

    normalized_text = normalize(text.lower())
    counts = {"fatalities": 0, "injured": 0, "evacuated": 0, "displaced": 0, "missing": 0}
    
    # Define improved regex patterns for victim extraction
    patterns = {
        "fatalities": [
            r"(\d+)\s*(?:pessoa(?:s)?)?\s*(?:morta(?:s)?|morto(?:s)?|falecida(?:s)?|óbito(?:s)?|vítima(?:s)?\s*morta(?:is)?)",
            r"(?:morreu|morreram|faleceu|faleceram)\s*(\d+)\s*(?:pessoa(?:s)?)?",
            r"(?:morto(?:s)?|falecido(?:s)?|óbito(?:s)?|vítima(?:s)?\s*morta(?:is)?)\s*(?:sendo|incluindo|contabilizando)?\s*(\d+)",
            r"(?:subiu|chega|chegou|aumentou)\s*(?:a|para)?\s*(\d+)(?:\s*o\s*(?:número|total)\s*de)?\s*(?:morto(?:s)?|vítima(?:s)?\s*morta(?:is)?)"
        ],
        "injured": [
            r"(\d+)\s*(?:pessoa(?:s)?)?\s*(?:ferida(?:s)?|ferido(?:s)?|hospitalizada(?:s)?|internada(?:s)?)",
            r"(?:feriu-se|feriram-se|ficaram\s*feridas?)\s*(\d+)\s*(?:pessoa(?:s)?)?",
            r"(?:ferido(?:s)?|hospitalizado(?:s)?|internado(?:s)?)\s*(?:sendo|incluindo|contabilizando)?\s*(\d+)"
        ],
        "evacuated": [
            r"(\d+)\s*(?:pessoa(?:s)?)?\s*(?:evacuada(?:s)?|retirada(?:s)?|tiveram\s*que\s*sair|foram\s*retiradas)",
            r"(?:evacuaram|retirou|retiraram|retiradas?)\s*(\d+)\s*(?:pessoa(?:s)?)?",
            r"evacuação\s*de\s*(\d+)\s*(?:pessoa(?:s)?)"
        ],
        "displaced": [
            r"(\d+)\s*(?:pessoa(?:s)?)?\s*(?:desalojada(?:s)?|desabrigada(?:s)?|ficaram\s*sem\s*casa|perderam\s*casa(?:s)?)",
            r"(?:desalojou|desabrigou)\s*(\d+)\s*(?:pessoa(?:s)?)?",
            r"(?:desalojados|desabrigados|pessoas\s*sem\s*casa)\s*(?:sendo|incluindo|contabilizando)?\s*(\d+)"
        ],
        "missing": [
            r"(\d+)\s*(?:pessoa(?:s)?)?\s*(?:desaparecida(?:s)?|não\s*encontrada(?:s)?|não\s*localizada(?:s)?)",
            r"(?:desapareceu|desapareceram)\s*(\d+)\s*(?:pessoa(?:s)?)?",
            r"(?:desaparecidos|pessoas\s*desaparecidas)\s*(?:sendo|incluindo|contabilizando)?\s*(\d+)"
        ]
    }
    
    # Search for patterns in the text
    for victim_type, pattern_list in patterns.items():
        max_count = 0
        for pattern in pattern_list:
            matches = re.finditer(pattern, normalized_text)
            for match in matches:
                # Get the captured group - the number
                number_str = match.group(1)
                if number_str.isdigit():
                    number = int(number_str)
                    # Validate the number is reasonable (not a year, not too large)
                    if 0 < number < 1000 and not (1900 <= number <= 2100):
                        # Get context to verify if it's a valid victim count
                        start_pos = max(0, match.start() - 50)
                        end_pos = min(len(normalized_text), match.end() + 50)
                        context = normalized_text[start_pos:end_pos]
                        
                        # Skip if context indicates this isn't a victim (negation, temperature, etc)
                        negation_terms = ["não há", "nenhum", "zero", "sem"]
                        false_contexts = ["graus", "temperatura", "km/h", "euros", "anos de idade", "anos atrás"]
                        
                        is_negated = any(term in context for term in negation_terms)
                        is_false_context = any(term in context for term in false_contexts)
                        
                        if not is_negated and not is_false_context:
                            max_count = max(max_count, number)
        
        counts[victim_type] = max_count
        
    # Check for word-based numbers (e.g., "dois mortos")
    word_num_patterns = {
        "fatalities": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:morta(?:s)?|morto(?:s)?|falecida(?:s)?|vítima(?:s)?\s*morta(?:is)?)",
        "injured": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:ferida(?:s)?|ferido(?:s)?)",
        "evacuated": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:evacuada(?:s)?|retirada(?:s)?)",
        "displaced": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:desalojada(?:s)?|desabrigada(?:s)?)",
        "missing": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:desaparecida(?:s)?)"
    }
    
    for victim_type, pattern in word_num_patterns.items():
        matches = re.finditer(pattern, normalized_text)
        for match in matches:
            word_num = match.group(1)
            number = word_to_number(word_num)
            if number and number > counts[victim_type]:
                counts[victim_type] = number
    
    return counts

def verify_victim_context(text, victim_count, victim_type):
    """
    Verifies if a victim count is valid based on surrounding context.
    Returns True if the context confirms this is a victim count.
    """
    # Skip verification for zero counts
    if victim_count == 0:
        return False
        
    victim_contexts = {
        "fatalities": ["mortos", "morto", "morta", "mortas", "falecido", "falecimento", 
                      "óbito", "vítima mortal", "vítimas mortais", "causou a morte"],
        "injured": ["ferido", "ferida", "feridos", "feridas", "hospitalizado", 
                   "internado", "lesionado", "traumatismo", "tratamento médico"],
        "evacuated": ["evacuado", "evacuada", "evacuados", "evacuadas", "retirado", 
                     "retirados", "saíram de casa", "tiveram de abandonar"],
        "displaced": ["desalojado", "desalojada", "desalojados", "desalojadas", 
                     "sem casa", "perderam casa", "desabrigados"],
        "missing": ["desaparecido", "desaparecida", "desaparecidos", "desaparecidas", 
                   "não encontrado", "não localizado", "paradeiro desconhecido"]
    }
    
    # Check if context contains terms related to the victim type
    contexts = victim_contexts.get(victim_type, [])
    normalized_text = normalize(text.lower())
    
    # Check for pattern: [number] + [victim type terms]
    victim_count_str = str(victim_count)
    for context_term in contexts:
        pattern = rf"{victim_count_str}\s+(?:\w+\s+)*?{context_term}"
        if re.search(pattern, normalized_text):
            return True
            
    # Check for pattern: [victim type terms] + [number]
    for context_term in contexts:
        pattern = rf"{context_term}(?:\w+\s+)*?{victim_count_str}"
        if re.search(pattern, normalized_text):
            return True
            
    # Default: if we can't verify, be conservative
    return False

def get_real_url_with_newspaper(link, driver_path="/usr/bin/chromedriver", max_wait_time=10):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)
#SEE NOTES:
    try:
        print(f"🌐 A aceder ao link: {link}")
        driver.get(link)
        wait = WebDriverWait(driver, max_wait_time)

        # Se for página de consentimento, tenta clicar no botão "Aceitar tudo"
        if "consent.google.com" in driver.current_url:
            print("⚠️ Página de consentimento detetada. A tentar aceitar...")

            try:
                # Try multiple approaches to accept consent
                # First approach - try to switch to iframe if it exists
                try:
                    iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    if iframes:
                        for iframe in iframes:
                            try:
                                driver.switch_to.frame(iframe)
                                accept_buttons = driver.find_elements(By.XPATH, 
                                    '//button[contains(., "Aceitar") or contains(., "Accept") or contains(., "agree")]')
                                if accept_buttons:
                                    accept_buttons[0].click()
                                    print("✅ Consentimento aceito via iframe!")
                                    driver.switch_to.default_content()
                                    break
                            except Exception:
                                driver.switch_to.default_content()
                                continue
                except Exception as e:
                    print(f"⚠️ Erro ao tentar iframe: {e}")
                
                # Second approach - try direct button on main page
                try:
                    accept_btn = wait.until(
                        EC.element_to_be_clickable((By.XPATH, 
                            '//button[contains(., "Aceitar tudo") or contains(., "Accept all") or contains(., "I agree")]'))
                    )
                    accept_btn.click()
                    print("✅ Consentimento aceito via botão principal!")
                except Exception:
                    # Third approach - try buttons with common IDs/attributes
                    buttons = driver.find_elements(By.CSS_SELECTOR, 
                                               'button[jsname="higCR"], button[id*="accept"], button[data-action*="accept"]')
                    if buttons:
                        buttons[0].click()
                        print("✅ Consentimento aceito via seletor CSS!")

            except Exception as e:
                print("❌ Não consegui aceitar o consentimento:", e)

        # Espera que o URL final mude e a página de destino carregue
        wait.until(lambda d: not d.current_url.startswith("https://consent.google.com"))
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        final_url = driver.current_url
        print(f"✅ URL final resolvido: {final_url}")
        return final_url

    except Exception as e:
        print(f"❌ Erro ao resolver URL com Selenium: {e}")
        return None
    finally:
        driver.quit()


def extract_article_content(url):
    """Extract article content using newspaper3k with improved victim extraction."""
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
    config = Config()
    config.browser_user_agent = user_agent
    config.fetch_images = False  # Don't download images to speed up

    article_data = {
        "article_title": "", 
        "article_text": "", 
        "fatalities": 0, 
        "injured": 0, 
        "evacuated": 0, 
        "displaced": 0, 
        "missing": 0
    }

    try:
        print(f"📰 Extraindo conteúdo do artigo de {url}...")
        article = Article(url, config=config)
        article.download()
        article.parse()

        article_data["article_title"] = article.title
        article_data["article_text"] = article.text
        
        # Extract victim counts from both title and text
        victim_counts_from_title = extract_victim_counts(article.title)
        victim_counts_from_text = extract_victim_counts(article.text)
        
        # Combine the results, taking the maximum values from each source
        for victim_type in ["fatalities", "injured", "evacuated", "displaced", "missing"]:
            title_count = victim_counts_from_title.get(victim_type, 0)
            text_count = victim_counts_from_text.get(victim_type, 0)
            
            # Verify context for high counts to reduce false positives
            if title_count > 0 and verify_victim_context(article.title, title_count, victim_type):
                article_data[victim_type] = title_count
            elif text_count > 0 and verify_victim_context(article.text, text_count, victim_type):
                article_data[victim_type] = text_count
            else:
                # Use the largest value but only if it's reasonable
                article_data[victim_type] = max(title_count, text_count) if max(title_count, text_count) < 100 else 0
        
        print("✅ Conteúdo do artigo extraído com sucesso!")
        
        # Additional verification for suspiciously large numbers
        for victim_type in ["fatalities", "injured", "evacuated", "displaced", "missing"]:
            count = article_data[victim_type]
            if count > 50:  # Large numbers need stronger verification
                # If we can't verify large numbers in context, set to 0 to avoid false data
                if not verify_victim_context(article.title, count, victim_type) and not verify_victim_context(article.text, count, victim_type):
                    print(f"⚠️ Contagem suspeita para {victim_type}: {count} - ignorando")
                    article_data[victim_type] = 0

    except Exception as e:
        print(f"❌ Erro ao extrair conteúdo do artigo de {url}: {e}")

    return article_data

def extrair_conteudo(link, timeout=10, driver_path="/usr/bin/chromedriver"):
    def fetch_content():
        # Tenta resolver via redirecionamento HTTP
        real_url = resolve_google_news_url(link) or resolve_with_newspaper(link) or link
        if not real_url:
            print(f"⚠️ Fallback para link original: {link}")
            real_url = link
        return fetch_and_extract_article_text(real_url)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(fetch_content)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            print(f"⚠️ Timeout ao extrair {link}")
            return ""
        except Exception as e:
            print(f"❌ Erro ao extrair: {e}")
            return ""


def load_freguesias_codigos(filepath):
    """
    Load freguesias and their corresponding codes from a CSV file.
    
    Args:
        filepath (str): Path to the CSV file containing freguesias and codes.
        
    Returns:
        dict: Dictionary with freguesia names as keys and their codes as values.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            if 'Freguesia' not in reader.fieldnames or 'Código' not in reader.fieldnames:
                raise ValueError("CSV file must contain 'Freguesia' and 'Código' columns.")
            return {normalize(row['Freguesia']): row['Código'] for row in reader}
    except FileNotFoundError:
        print(f"⚠️ O arquivo {filepath} não foi encontrado.")
        return {}
    except ValueError as e:
        print(f"⚠️ Erro no formato do arquivo CSV: {e}")
        return {}
    except Exception as e:
        print(f"❌ Erro ao carregar os códigos de freguesias: {e}")
        return {}

def get_real_url_and_content(link, driver_path="/usr/bin/chromedriver", max_wait_time=5):
    """
    Retrieves the original URL and the content of the news article.
    """
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    page_data = {"source_url": None, "article_content": None}

    try:
        print(f"🌐 Acessando o link: {link}")
        driver.get(link)
        wait = WebDriverWait(driver, max_wait_time)

        # Handle consent page
        current_url = driver.current_url
        if current_url.startswith("https://consent.google.com/"):
            print("⚠️ Detetado consentimento explícito. A tentar aceitar...")
            try:
                accept_all_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, '//button[.//span[text()="Accept all"]]'))
                )
                if accept_all_button:
                    accept_all_button.click()
                    print("✅ Consentimento aceite!")
            except Exception:
                print("❌ Não foi possível localizar o botão de consentimento.")

        # Wait for redirection to the source website
        print("🔄 Redirecionando para o site de origem...")
        wait.until(lambda driver: not driver.current_url.startswith("https://news.google.com/")
                                and not driver.current_url.startswith("https://consent.google.com/"))

        # Wait for the article page to load
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # Get the final URL
        page_data["source_url"] = driver.current_url
        print(f"✅ URL final obtida: {page_data['source_url']}")

        # Extract article content using newspaper3k
        page_data["article_content"] = extract_article_content(page_data["source_url"])

    except Exception as e:
        print(f"❌ Erro ao obter URL de origem ou conteúdo para {link}: {e}")
        # Fallback to requests-based method
        page_data["source_url"] = get_original_url_via_requests(link)
        if page_data["source_url"]:
            page_data["article_content"] = extract_article_content(page_data["source_url"])

    finally:
        driver.quit()

    return page_data

def extract_with_selenium(url: str) -> str:
    """
    Extracts article content using Selenium when static extraction fails.
    """
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        
        # Wait for content to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "article"))
        )
        
        # Get the page source after JavaScript execution
        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()
        
        # Use the same extraction logic as before
        article_content = fetch_and_extract_article_text(soup)
        return article_content

    except Exception as e:
        print(f"❌ Selenium extraction failed: {str(e)}")
        return ""

def extract_victims_from_title(title):
    """
    Extracts victim counts from the title of a news article with improved accuracy.
    Returns a dictionary with counts for different victim types.
    """
    if not title:
        return {
            "fatalities": 0,
            "injured": 0,
            "evacuated": 0,
            "displaced": 0,
            "missing": 0
        }
    
    normalized_title = normalize(title.lower())
    counts = {
        "fatalities": 0,
        "injured": 0,
        "evacuated": 0,
        "displaced": 0,
        "missing": 0
    }
    
    # Improved patterns for titles specifically
    patterns = {
        "fatalities": [
            r"(\d+)\s*mort[eo]s?",
            r"(\d+)\s*vítimas?\s*mortais?",
            r"(\d+)\s*óbitos?",
            r"(\d+)\s*falecid[oa]s?",
            r"morre(?:m|ram|u)?\s*(\d+)",
            r"mata\s*(\d+)",
            r"causa\s*(\d+)\s*mort(?:es|ais)",
            r"mortal(?:idade)?\s*de\s*(\d+)"
        ],
        "injured": [
            r"(\d+)\s*ferid[oa]s?",
            r"(\d+)\s*pessoas?\s*feridas?",
            r"fere\s*(\d+)",
            r"feriu\s*(\d+)",
            r"(\d+)\s*lesionad[oa]s?",
            r"(\d+)\s*hospitalizad[oa]s?"
        ],
        "evacuated": [
            r"(\d+)\s*evacuad[oa]s?",
            r"(\d+)\s*pessoas?\s*evacuadas?",
            r"evacua[mç][ãa]o\s*de\s*(\d+)",
            r"retirad[oa]s?\s*(\d+)",
            r"retira[mr]?\s*(\d+)"
        ],
        "displaced": [
            r"(\d+)\s*desalojad[oa]s?",
            r"(\d+)\s*pessoas?\s*desalojadas?",
            r"(\d+)\s*pessoas?\s*sem\s*casa",
            r"(\d+)\s*desabrigad[oa]s?",
            r"deixa\s*(\d+)\s*sem\s*casa"
        ],
        "missing": [
            r"(\d+)\s*desaparecid[oa]s?",
            r"(\d+)\s*pessoas?\s*desaparecidas?",
            r"(\d+)\s*pessoas?\s*desaparecem",
            r"desaparecem\s*(\d+)"
        ]
    }
    
    # Search for patterns in the title
    for victim_type, pattern_list in patterns.items():
        for pattern in pattern_list:
            matches = re.finditer(pattern, normalized_title)
            for match in matches:
                # Get the captured group - the number
                number_str = match.group(1)
                if number_str.isdigit():
                    number = int(number_str)
                    # Validate the number is reasonable (not a year, not too large)
                    if 0 < number < 1000 and not (1900 <= number <= 2100):
                        counts[victim_type] = max(counts[victim_type], number)
    
    # Check for word-based numbers (e.g., "dois mortos")
    word_num_patterns = {
        "fatalities": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:morta(?:s)?|morto(?:s)?|falecida(?:s)?|vítima(?:s)?\s*morta(?:is)?)",
        "injured": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:ferida(?:s)?|ferido(?:s)?)",
        "evacuated": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:evacuada(?:s)?|retirada(?:s)?)",
        "displaced": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:desalojada(?:s)?|desabrigada(?:s)?)",
        "missing": r"(um|uma|dois|duas|três|tres|quatro|cinco|seis|sete|oito|nove|dez)\s+(?:pessoa(?:s)?)?\s*(?:desaparecida(?:s)?)"
    }
    
    for victim_type, pattern in word_num_patterns.items():
        matches = re.finditer(pattern, normalized_title)
        for match in matches:
            word_num = match.group(1)
            number = word_to_number(word_num)
            if number and number > counts[victim_type]:
                counts[victim_type] = number
    
    return counts