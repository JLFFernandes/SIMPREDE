from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess

# Caminho correto para o script Python
SCRIPT_PATH = "/usr/local/airflow/scripts/pts_disaster_export.py"

def run_pts_script():
    if not os.path.exists(SCRIPT_PATH):
        raise FileNotFoundError(f"Script não encontrado: {SCRIPT_PATH}")
    
    # Usa subprocess com check=True para garantir falha no Airflow se o script falhar
    result = subprocess.run(["python3", SCRIPT_PATH], check=True, capture_output=True, text=True)
    print(result.stdout)  # Mostra stdout nos logs do Airflow
    print(result.stderr)  # Mostra stderr nos logs do Airflow (útil para debug)

    return "Script executado com sucesso"

# Default args para o DAG
default_args = {
    'owner': 'simprede',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'retries': 1,
}

# Definição do DAG
with DAG(
    dag_id='pts_disaster_manual',
    default_args=default_args,
    description='Executa manualmente o script pts_disaster_export.py',
    schedule=None,  # Fixed: changed from schedule_interval to schedule
    catchup=False,
) as dag:

    executar_pts_import = PythonOperator(
        task_id='executar_pts_import',
        python_callable=run_pts_script,
    )

    executar_pts_import
    dag = dag
