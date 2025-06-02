from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess
import sys

def validate_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        print(f"⚠️ Formato de data inválido: {date_str}. Use AAAA-MM-DD")
        return None

def run_script(script_path, dias=1, date=None):
    """Generic function to run a Python script with parameters"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    full_script_path = os.path.join(base_dir, 'google_scraper', script_path)
    
    # First check if the script exists
    if not os.path.exists(full_script_path):
        print(f"⚠️ Script não encontrado em: {full_script_path}")
        raise FileNotFoundError(f"Script não encontrado em: {full_script_path}")
    
# First ensure required packages are installed
    try:
        # Install dependencies with specific versions to avoid compatibility issues
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--user", 
             "feedparser", 
             "requests", 
             "beautifulsoup4",
             "selenium==4.21.0",            # Explicitly specify selenium version
             "selenium-wire==5.1.0",        # Add selenium-wire for advanced selenium features
             "webdriver-manager==3.8.6",    # For automated webdriver management
             "tenacity", 
             "aiohttp", 
             "psycopg2-binary", 
             "python-dotenv",
             "pandas",
             "lxml"],
            check=True,
            capture_output=True,
            text=True
        )
        print("✅ Dependências instaladas com sucesso")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Falha ao instalar dependências: {e}")
        print(f"Erro: {e.stderr}")
    
    cmd = ["python3", full_script_path]
    
    # Add dias parameter for scripts that need it
    if script_path in [
        "scraping/run_scraper.py",
        "scraping/run_scraper_airflow.py",
        "processador/processar_relevantes.py",
        "processador/processar_relevantes_airflow.py",
        "processador/filtrar_artigos_vitimas.py",
        "processador/filtrar_artigos_vitimas_airflow.py"
    ]:
        cmd.extend(["--dias", str(dias)])
    
    # Add date parameter if provided and not None or "None"
    if date and date != "None" and script_path in [
        "scraping/run_scraper.py",
        "scraping/run_scraper_airflow.py",
        "processador/processar_relevantes.py",
        "processador/processar_relevantes_airflow.py",
        "processador/filtrar_artigos_vitimas.py",
        "processador/filtrar_artigos_vitimas_airflow.py",
        "exportador_bd/export_to_supabase_airflow.py"
    ]:
        cmd.extend(["--date", date])
    
    print(f"📦 A executar comando: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(f"Código de retorno: {result.returncode}")
        print(f"Resultado: {result.stdout}")
        if result.stderr:
            print(f"Erros: {result.stderr}")
        
        if result.returncode != 0:
            print(f"⚠️ Comando falhou com código de retorno {result.returncode}")
            raise subprocess.CalledProcessError(
                result.returncode, cmd, 
                output=result.stdout, 
                stderr=result.stderr
            )
        
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Erro ao executar comando: {e}")
        print(f"Resultado: {e.output}")
        print(f"Erro: {e.stderr}")
        raise

# Run the scraper
def run_scraper(**kwargs):
    dias = kwargs.get('dias', 1)
    date = kwargs.get('date', None)
    try:
        # Use the Airflow-specific version
        return run_script("scraping/run_scraper_airflow.py", dias, date)
    except Exception as e:
        print(f"⚠️ Erro em run_scraper: {e}")
        # Fall back to original script if the airflow version fails
        try:
            return run_script("scraping/run_scraper.py", dias, date)
        except Exception as e2:
            print(f"⚠️ Erro no fallback run_scraper: {e2}")
            raise

# Process relevant articles
def processar_relevantes(**kwargs):
    dias = kwargs.get('dias', 1)
    date = kwargs.get('date', None)
    try:
        return run_script("processador/processar_relevantes_airflow.py", dias, date)
    except Exception as e:
        print(f"⚠️ Erro em processar_relevantes: {e}")
        # Fall back to original script if the airflow version fails
        try:
            return run_script("processador/processar_relevantes.py", dias, date)
        except Exception as e2:
            print(f"⚠️ Erro no fallback processar_relevantes: {e2}")
            raise

# Filter articles about victims
def filtrar_artigos_vitimas(**kwargs):
    dias = kwargs.get('dias', 1)
    date = kwargs.get('date', None)
    try:
        return run_script("processador/filtrar_artigos_vitimas_airflow.py", dias, date)
    except Exception as e:
        print(f"⚠️ Erro em filtrar_artigos_vitimas: {e}")
        # Fall back to original script if the airflow version fails
        try:
            return run_script("processador/filtrar_artigos_vitimas.py", dias, date)
        except Exception as e2:
            print(f"⚠️ Erro no fallback filtrar_artigos_vitimas: {e2}")
            raise

# Export to Supabase
def export_to_supabase(**kwargs):
    date = kwargs.get('date', None)
    try:
        # Use the Airflow-specific version
        return run_script("exportador_bd/export_to_supabase_airflow.py", date=date)
    except Exception as e:
        print(f"⚠️ Erro em export_to_supabase: {e}")
        # Fall back to original script if the airflow version fails
        try:
            return run_script("exportador_bd/export_to_supabase.py")
        except Exception as e2:
            print(f"⚠️ Erro no fallback export_to_supabase: {e2}")
            raise

# Test Selenium environment
def test_selenium_environment():
    """Test if Selenium and Chrome are properly configured."""
    import logging
    import sys
    import os
    
    logging.info("📋 A testar ambiente Selenium/Chrome")
    
    try:
        # Test 1: Import Selenium directly in the current Python environment
        logging.info("🔍 Testing Selenium import in current environment...")
        import selenium
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        logging.info(f"✅ Selenium {selenium.__version__} imported successfully")
        
        # Test 2: Check Chrome/ChromeDriver
        chrome_bin = os.environ.get('CHROME_BIN', '/usr/bin/chromium')
        chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
        
        if os.path.exists(chrome_bin):
            logging.info(f"✅ Chrome/Chromium found at: {chrome_bin}")
        else:
            logging.error(f"❌ Chrome/Chromium not found at: {chrome_bin}")
            return False
            
        if os.path.exists(chromedriver_path):
            logging.info(f"✅ ChromeDriver found at: {chromedriver_path}")
        else:
            logging.error(f"❌ ChromeDriver not found at: {chromedriver_path}")
            return False
        
        # Test 3: Try to create a headless Chrome instance
        logging.info("🚀 Testing headless Chrome...")
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--remote-debugging-port=9222')
        chrome_options.binary_location = chrome_bin
        
        service = Service(chromedriver_path)
        
        try:
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.get("data:text/html,<html><body><h1>Test</h1></body></html>")
            title = driver.title
            driver.quit()
            logging.info(f"✅ Headless Chrome test successful - page title: {title}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Headless Chrome test failed: {str(e)}")
            return False
            
    except ImportError as e:
        logging.error(f"❌ Selenium import error: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"❌ Selenium test failed: {str(e)}")
        return False

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'google_scraper_pipeline',
    default_args=default_args,
    description='Pipeline de scraping e processamento de notícias',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['scraper', 'news'],
) as dag:
    
    # Create tasks with default parameter values
    t0 = PythonOperator(
        task_id='test_selenium_environment',
        python_callable=test_selenium_environment,  # Fixed: was test_selenium_env
    )
    
    t1 = PythonOperator(
        task_id='run_scraper',
        python_callable=run_scraper,
        op_kwargs={
            'dias': "{{ dag_run.conf.get('dias', 1) }}",
            'date': "{{ dag_run.conf.get('date', '') }}",
        },
    )
    
    t2 = PythonOperator(
        task_id='processar_relevantes',
        python_callable=processar_relevantes,
        op_kwargs={
            'dias': "{{ dag_run.conf.get('dias', 1) }}",
            'date': "{{ dag_run.conf.get('date', '') }}",
        },
    )
    
    t3 = PythonOperator(
        task_id='filtrar_artigos_vitimas',
        python_callable=filtrar_artigos_vitimas,
        op_kwargs={
            'dias': "{{ dag_run.conf.get('dias', 1) }}",
            'date': "{{ dag_run.conf.get('date', '') }}",
        },
    )
    
    t4 = PythonOperator(
        task_id='export_to_supabase',
        python_callable=export_to_supabase,
        op_kwargs={
            'date': "{{ dag_run.conf.get('date', '') }}",
        },
    )
    
    # Set dependencies
    t0 >> t1 >> t2 >> t3 >> t4
