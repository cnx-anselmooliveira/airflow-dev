import os
from airflow import DAG
from models.trino_to_sftp import TrinoToSftp
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'Rafael Soufraz',
    'start_date': datetime(2024, 7, 28),
}

with DAG(
    dag_id='teste-trino-sftp',
    schedule_interval='0 8 * * 0',
    catchup=False,
    default_args=default_args
) as dag:

    dags_folder = os.environ['AIRFLOW__CORE__DAGS_FOLDER']
    ds_format = """{{ dag_run.queued_at.strftime('%Y%m%d') }}"""

    QUERIES_PATHS = [
        f'{dags_folder}/queries/teste.sql',
        ]

    LOCAL_CSV_PATHS = [
        '/tmp/teste.csv',
        ]

    REMOTE_SFTP_PATHS = [
        f'stone/gestao_saude/atendimentos_diario_{ds_format}.csv',
        ]
    
    VARIABLE_TEMPLATES = [
        'stone_max_data_atendimentos_diario',
    ]

    INCREMENTAL_FIELDS = [
        'data_hora_ultima_atualizacao',
    ]

    task_trino_to_sftp = TrinoToSftp(
        task_id="task_trino_to_sftp",
        trino_conn_id="trino_conn",
        sftp_conn_id="sftp_stone",
        query_paths=QUERIES_PATHS,
        local_csv_paths=LOCAL_CSV_PATHS,
        remote_sftp_paths=REMOTE_SFTP_PATHS,
        variable_templates=VARIABLE_TEMPLATES 
    )