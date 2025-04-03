import time
import tempfile
import shutil
import uuid
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def get_real_url(link, wait_time=3):
    temp_dir = tempfile.mkdtemp(prefix=f"chrome_temp_{uuid.uuid4()}_")

    options = Options()
    options.add_argument(f"--user-data-dir={temp_dir}")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--headless")  # podes comentar esta linha para ver o browser

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(link)
        time.sleep(1)

        if "consent.google.com" in driver.current_url:
            print("⚠️ Detetado consentimento explícito. A tentar aceitar...")
            try:
                consent_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//form//button[@type="submit"]'))
                )
                driver.execute_script("arguments[0].click();", consent_button)
                print("✅ Consentimento aceite via JS!")
                time.sleep(wait_time)
            except Exception as e:
                print(f"❌ Erro ao aceitar consentimento: {e}")
        else:
            print("ℹ️ Página de consentimento não apareceu (ótimo ou já aceite).")
            time.sleep(wait_time)

        final_url = driver.current_url

    except Exception as e:
        print(f"[Erro ao abrir link com Selenium]: {e}")
        final_url = link
    finally:
        driver.quit()
        shutil.rmtree(temp_dir, ignore_errors=True)

    return final_url


# Teste
link_google_news = "https://news.google.com/rss/articles/CBMiY2h0dHBzOi8vd3d3LmV4YW1wbGUuY29tL25vdGljaWEtdGVzdGUvYXJ0aWdvLW1vemFtcW91ZS1pbm9uZGFjaW8_b2M9NSZobD1wdC1QVCZnbD1QVCZjZWlkPVBUOnB0NtIBAA?oc=5"
url_real = get_real_url(link_google_news)
print("➡️ Link final:", url_real)
