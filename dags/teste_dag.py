import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator  # Substitui DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Test Owner',
    'start_date': datetime(2025, 9, 16),
}

# Função Python para log
def log_message():
    logger.info("Tarefa Python executada com sucesso!")

with DAG(
    dag_id='dag_teste_airflow_3',
    schedule=None,  # DAG manual para testes
    catchup=False,
    default_args=default_args,
    tags=['test', 'example'],
    description='DAG de teste para o Airflow 3'
) as dag:

    # Tarefa inicial (EmptyOperator substitui DummyOperator)
    start_task = EmptyOperator(
        task_id='start_task'
    )

    # Tarefa Bash
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Executando tarefa Bash!"'
    )

    # Tarefa Python
    log_task = PythonOperator(
        task_id='log_task',
        python_callable=log_message
    )

    # Tarefa final
    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Definição do fluxo de execução
    start_task >> bash_task >> log_task >> end_task
