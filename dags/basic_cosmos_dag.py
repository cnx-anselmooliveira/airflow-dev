"""
An example DAG that uses Cosmos to render a dbt project into an Airflow DAG.
"""

import os
from datetime import datetime
from pathlib import Path
import logging

# [START cosmos_init_imports]
from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig

# [END cosmos_init_imports]
from cosmos.profiles import TrinoLDAPProfileMapping

from cosmos.constants import ExecutionMode, LoadMode


DBT_PROJECT_PATH = Path(os.getenv("DBT_PROJECT_DIR", '/opt/airflow/dags/repo/dbt'))

logging.info(f'DBT_PROJECT_PATH: {DBT_PROJECT_PATH}')
#/opt/airflow/dbt/conexa_dbt_dados/dbt_project.yml

profile_config = ProfileConfig(
    profile_name="conexa_dbt",       # deve bater com o nome no profiles.yml do repo dbt
    target_name="prod",
    profile_mapping=TrinoLDAPProfileMapping(
        conn_id="trino_conn",      # Airflow Connection ID configurado na UI
        profile_args={
            "schema": "delta",
            "catalog": "lakehouse",
        },
    ),
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    dbt_project_path=DBT_PROJECT_PATH,
    # select=["tag:daily"],   # filtra por tag/pasta se necessário
    # exclude=["tag:skip"],
    emit_datasets=False,
)

# [START local_example]
basic_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    render_config=render_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="basic_cosmos_dag",
    default_args={"retries": 0},
)
# [END local_example]

basic_cosmos_dag