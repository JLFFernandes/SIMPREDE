from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'simprede',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'retries': 1,
}

with DAG(
    dag_id='emdat_final_manual',
    default_args=default_args,
    description='Executa manualmente o script EM-DAT_final.py com Selenium',
    schedule=None,  # <--- ALTERADO
    catchup=False,
) as dag:

    run_emdat_final = BashOperator(
        task_id='run_emdat_final',
        bash_command='bash -c "python3 /usr/local/airflow/scripts/EM-DAT_final.py"',
    )

    run_emdat_final
