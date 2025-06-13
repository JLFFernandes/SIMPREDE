# File: airflow-GOOGLE-NEWS-SCRAPER/scripts/google_scraper/tests/test_selenium.py
# Script para testar o Selenium com Chrome em modo headless

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument("--headless")  # <- usa o modo antigo
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.binary_location = "/usr/bin/chromium"

service = Service("/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=options)

driver.set_page_load_timeout(10)  # timeout para evitar bloqueio

try:
    driver.get("https://www.google.com")
    print(driver.title)
except Exception as e:
    print(f"❌ Erro ao carregar página: {e}")

driver.quit()