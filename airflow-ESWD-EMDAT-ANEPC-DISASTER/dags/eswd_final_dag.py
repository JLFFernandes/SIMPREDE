from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default args para o DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'retries': 1,
}

# DAG para importação do ESWD manual
with DAG(
    dag_id='eswd_final_manual',
    default_args=default_args,
    description='Executa manualmente o script ESWD_final.py (importa CSV para Supabase)',
    schedule=None,
    catchup=False,
) as dag:

    run_eswd_import = BashOperator(
        task_id='run_eswd_import',
        bash_command='python /usr/local/airflow/scripts/ESWD_final.py',
    )

    run_eswd_import
