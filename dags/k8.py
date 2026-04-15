"""
DAG: dbt via Astronomer Cosmos no Kubernetes (Conexa Saúde)
============================================================
Executor   : CeleryExecutor (setup atual)
dbt pods   : KubernetesPodOperator via Cosmos (1 pod por node dbt)
Projeto dbt: init-container faz git clone → emptyDir compartilhado
             (repo separado do airflow_dags.git)

Dependências no requirements do Airflow (imagem ECR):
  astronomer-cosmos[dbt-core]>=1.4.0
  apache-airflow-providers-cncf-kubernetes>=8.0.0

ATENÇÃO — ajuste as variáveis da seção "Configurações" abaixo
antes de commitar no airflow_dags.git (branch main).
"""

from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.config import ProfileConfig
from cosmos.constants import ExecutionMode, LoadMode

from cosmos.profiles import TrinoLDAPProfileMapping
from kubernetes.client import models as k8s

# ---------------------------------------------------------------------------
# Configurações — ajuste para o seu ambiente
# ---------------------------------------------------------------------------

# Namespace onde o Airflow está rodando no EKS
K8S_NAMESPACE = "airflow"

# Imagem dbt no ECR (crie uma imagem com dbt + adapter instalados)
# Ex: FROM ghcr.io/dbt-labs/dbt-trino:1.8.0  → push para ECR
DBT_IMAGE = "ghcr.io/dbt-labs/dbt-trino:latest"

# Repo git do projeto dbt (separado do airflow_dags.git)
DBT_GIT_REPO = "https://github.com/conexasaude/airflow.git"
DBT_GIT_BRANCH = "main"

# Scheduler: path disponível via git-sync (usado no parse da DAG)
SCHEDULER_DBT_PATH = Path("/opt/airflow/dbt/conexa_dbt_dados")

# Pod: raiz do clone + subpasta do projeto dbt (usado na execução)
CLONE_ROOT = Path("/dbt/project")
POD_DBT_PATH = Path("/opt/airflow/dbt/conexa_dbt_dados")

# ---------------------------------------------------------------------------
# Volumes — emptyDir compartilhado entre init-container e container dbt
# ---------------------------------------------------------------------------

dbt_empty_dir = k8s.V1Volume(
    name="dbt-project",
    empty_dir=k8s.V1EmptyDirVolumeSource(),
)

dbt_volume_mount = k8s.V1VolumeMount(
    name="dbt-project",
    mount_path="/dbt",
)

# ---------------------------------------------------------------------------
# Init container — clona o projeto dbt antes do node dbt rodar
#
# Os pods gerados pelo KubernetesPodOperator são independentes dos workers
# Celery e não têm acesso ao volume de git-sync dos workers. Por isso
# usamos um init-container dedicado por pod.
# ---------------------------------------------------------------------------

git_clone_init_container = k8s.V1Container(
    name="git-clone-dbt",
    image="alpine/git:2.43.0",
    command=[
        "sh", "-c",
        f"git clone --depth 1 --branch {DBT_GIT_BRANCH} {DBT_GIT_REPO} {CLONE_ROOT}",
    ],
    volume_mounts=[dbt_volume_mount],
    # Se o repo for privado, injete o token do mesmo secret do git-sync:
    # env=[
    #     k8s.V1EnvVar(
    #         name="GIT_TOKEN",
    #         value_from=k8s.V1EnvVarSource(
    #             secret_key_ref=k8s.V1SecretKeySelector(
    #                 name="airflow-git-secret",
    #                 key="password",
    #             )
    #         ),
    #     ),
    # ],
    # command para repo privado:
    # command=["sh", "-c",
    #   f"git clone --depth 1 --branch {DBT_GIT_BRANCH} "
    #   f"https://oauth2:$(GIT_TOKEN)@github.com/conexasaude/dbt_project.git"
    #   f" {DBT_CLONE_PATH}"],
)

# ---------------------------------------------------------------------------
# Node Affinity — igual ao que está no values.yaml (força nós amd64)
# ---------------------------------------------------------------------------

node_affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
            node_selector_terms=[
                k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(
                            key="kubernetes.io/arch",
                            operator="In",
                            values=["amd64"],
                        )
                    ]
                )
            ]
        )
    )
)

# ---------------------------------------------------------------------------
# ProfileConfig — mapeamento de conexão dbt
# ---------------------------------------------------------------------------

profile_config = ProfileConfig(
    profile_name="conexa_dbt",       # deve bater com o nome no profiles.yml do repo dbt
    target_name="prod",
    profile_mapping=TrinoLDAPProfileMapping(
        conn_id="trino_default",      # Airflow Connection ID configurado na UI
        profile_args={
            "schema": "delta",
            "catalog": "lakehouse",
        },
    ),
)

# ---------------------------------------------------------------------------
# ProjectConfig — aponta para o path clonado pelo init-container
# ---------------------------------------------------------------------------

project_config = ProjectConfig(
    models_relative_path="models",
    seeds_relative_path="seeds",
    snapshots_relative_path="snapshots",
)

# ---------------------------------------------------------------------------
# ExecutionConfig
# ---------------------------------------------------------------------------

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.KUBERNETES,
    dbt_project_path=POD_DBT_PATH,
    dbt_executable_path="/usr/local/bin/dbt",          # caminho no pod (dbt-trino:latest)
)

# ---------------------------------------------------------------------------
# RenderConfig
# ---------------------------------------------------------------------------

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    dbt_project_path=SCHEDULER_DBT_PATH,
    dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",  # venv no dag-processor
    # select=["tag:daily"],   # filtra por tag/pasta se necessário
    # exclude=["tag:skip"],
    emit_datasets=False,
)

# ---------------------------------------------------------------------------
# Kwargs do KubernetesPodOperator (repassados pelo Cosmos a cada node dbt)
# ---------------------------------------------------------------------------

kubernetes_operator_args = {
    "image": DBT_IMAGE,
    "namespace": K8S_NAMESPACE,
    "image_pull_policy": "Always",           # mesmo padrão dos workers no values.yaml
    "get_logs": True,
    "is_delete_operator_pod": True,          # limpa pod após execução
    "in_cluster": True,                      # Airflow roda dentro do EKS

    "volumes": [dbt_empty_dir],
    "volume_mounts": [dbt_volume_mount],
    "init_containers": [git_clone_init_container],
    "affinity": node_affinity,               # força nós amd64

    # Recursos — ajuste conforme o tamanho dos seus modelos dbt
    "container_resources": k8s.V1ResourceRequirements(
        requests={"memory": "512Mi", "cpu": "250m"},
        limits={"memory": "1Gi", "cpu": "500m"},
    ),

    # Variáveis de ambiente no pod dbt
    "env_vars": [
        k8s.V1EnvVar(name="DBT_ENV", value="prod"),
        # Injete credenciais via External Secrets (mesmo padrão do values.yaml):
        # k8s.V1EnvVar(
        #     name="DBT_PASSWORD",
        #     value_from=k8s.V1EnvVarSource(
        #         secret_key_ref=k8s.V1SecretKeySelector(
        #             name="conexa-external-secrets-dbt",
        #             key="db_password",
        #         )
        #     ),
        # ),
    ],

    # Se o service account não tiver IRSA para o ECR, adicione o pull secret:
    # "image_pull_secrets": [k8s.V1LocalObjectReference(name="ecr-pull-secret")],

    # Labels para rastreabilidade no cluster
    "labels": {
        "app.kubernetes.io/instance": "conexa-airflow-prd",
        "app.kubernetes.io/component": "dbt-task",
    },
}

# ---------------------------------------------------------------------------
# DbtDag — DAG gerada automaticamente pelo Cosmos
# ---------------------------------------------------------------------------

dbt_dag = DbtDag(
    dag_id="dbt_cosmos_k8s",
    description="Executa o projeto dbt via Cosmos no EKS (1 pod por node dbt)",

    schedule="0 6 * * *",    # 06:00 UTC = 03:00 BRT
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,

    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    operator_args=kubernetes_operator_args,

    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
        "email_on_failure": False,
    },
    tags=["dbt", "cosmos", "kubernetes"],
)