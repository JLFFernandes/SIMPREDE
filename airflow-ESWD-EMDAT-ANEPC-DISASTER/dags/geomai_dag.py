from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

SCRIPT_PATH = '/usr/local/airflow/scripts/geomai_final.py'

def run_geomai_script():
    try:
        result = subprocess.run(
            ['python3', SCRIPT_PATH],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(f"Saída do script:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Erro ao executar o script:\n{e.stderr}")
        raise

with DAG(
    dag_id='geomai_diario',
    description='Executa o script geomai_final.py diariamente',
    schedule='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['geomai'],
) as dag:


    executar_script = PythonOperator(
        task_id='executar_geomai',
        python_callable=run_geomai_script,
    )
