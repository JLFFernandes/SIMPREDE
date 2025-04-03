import time
import tempfile
import shutil
import uuid
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse, parse_qs, urlencode
from newspaper import Article, Config
import json

def get_real_url_with_newspaper(link, driver_path, max_wait_time=5):
    options = Options()
    options.add_argument("--headless")  # Run in headless mode

    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    page_data = dict()

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

    except Exception as e:
        print(f"❌ Erro ao obter URL de origem para {link}: {e}")
        # Fallback to requests-based method
        page_data["source_url"] = get_original_url_via_requests(link)

    finally:
        driver.quit()

    return page_data["source_url"]


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


def extract_article_content(url):
    """Extract article content using newspaper3k."""
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
    config = Config()
    config.browser_user_agent = user_agent

    article_data = dict()

    try:
        print(f"📰 Extraindo conteúdo do artigo de {url}...")
        article = Article(url, config=config)
        article.download()
        article.parse()

        article_data["article_title"] = article.title
        article_data["article_text"] = article.text
        print("✅ Conteúdo do artigo extraído com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao extrair conteúdo do artigo de {url}: {e}")

    return article_data


# Teste
if __name__ == "__main__":
    # Update the driver path for Linux
    driver_path = "/usr/bin/chromedriver"  # Ensure this path is correct for your system
    google_rss_url = "https://news.google.com/rss/articles/CBMiugFBVV95cUxQaFpFZnlUcnhrNVJFd05SY1Y3dzRlaWNzRXFIQ2RaSGNvalFDTnpqSllKZXhicXVCX1VuOTFBOUJsY0NiUk1Ea1ExLWt0Q0stVUdwd2VWcG5LUUhYSWNGM2lNaW11cHJrdjNWRDlvYVktb2NqQ2pDTzVOWDFGX0o0VDAwSXh4STRaM2pyV01BS0ZuYmlFV3hfcnBuT2ZWNFgtcXpPdUpNZE91UkE1dldGUm0tcHlId2lxTmc?oc=5"

    # Get the real URL
    real_url = get_real_url_with_newspaper(google_rss_url, driver_path)
    print("➡️ URL final:", real_url)

    # Extract article content
    article_content = extract_article_content(real_url)
    print("📝 Conteúdo do artigo:", article_content)
