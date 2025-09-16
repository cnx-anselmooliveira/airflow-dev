from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'Rafael Soufraz',
    'start_date': datetime(2024, 7, 28),
}

with DAG(
        dag_id='_david2_zk_etl_dummy_test',
        default_args=default_args,
        schedule_interval='0 1 * * *',
        catchup=False
) as dag:
    start = DummyOperator(
        task_id='trigger_start'
    )
    
    mongo_extraction = DummyOperator(
        task_id='trigger_zk_mongo_extraction'
    )

    dbt_core = DummyOperator(
        task_id='trigger_zk_dbt'
    )

    promptuary_quality = DummyOperator(
        task_id='trigger_zk_promptuary_quality'
    )

    summary_credits_transactions = DummyOperator(
        task_id='trigger_zk_summary_credits_transactions'
    )

    bq_product_qualified_leads_to_hubspot = DummyOperator(
        task_id='trigger_bq_product_qualified_leads_to_hubspot'
    )

    bq_to_dal_visao_populacional = DummyOperator(
        task_id='trigger_zk_bq_to_dal_visao_populacional'
    )

    publico_profissionais_to_dal = DummyOperator(
        task_id='trigger_zk_publico_profissionais_to_dal'
    )

    sankhya_notas_to_dal = DummyOperator(
        task_id='trigger_zk_sankhya_notas_to_dal'
    )

    sankhya_integracao_vendas_e_compras = DummyOperator(
        task_id='trigger_zk_sankhya_integracao_vendas_e_compras'
    )

    looker_engagement_charger_to_hubspot = DummyOperator(
        task_id='trigger_zk_looker_engagement_charger_to_hubspot'
    )

    bq_to_dal_avoided_cost = DummyOperator(
        task_id='trigger_zk_bq_to_dal_avoided_cost'
    )

    bq_to_dal_sumario_executivo = DummyOperator(
        task_id='trigger_zk_bq_to_dal_sumario_executivo'
    )

    bq_to_dal_sumario_executivo_12_meses = DummyOperator(
        task_id='trigger_zk_bq_to_dal_sumario_executivo_12_meses'
    )

    bq_to_dal_enhanced_user_profile = DummyOperator(
        task_id='trigger_zk_enhanced_user_profile_to_dal'
    )

    zk_in_cnx_process = DummyOperator(
        task_id='trigger_zk_in_cnx_process'
    )

    # Definindo dependências
    start >> [mongo_extraction, zk_in_cnx_process]
    mongo_extraction >> dbt_core
    dbt_core >> [promptuary_quality, summary_credits_transactions, bq_product_qualified_leads_to_hubspot,
                 bq_to_dal_visao_populacional, bq_to_dal_sumario_executivo, bq_to_dal_sumario_executivo_12_meses,
                 bq_to_dal_enhanced_user_profile]
    [promptuary_quality, summary_credits_transactions, bq_product_qualified_leads_to_hubspot,
     bq_to_dal_visao_populacional, bq_to_dal_sumario_executivo, bq_to_dal_sumario_executivo_12_meses,
     bq_to_dal_enhanced_user_profile] >> publico_profissionais_to_dal >> sankhya_notas_to_dal
    sankhya_notas_to_dal >> sankhya_integracao_vendas_e_compras >> [looker_engagement_charger_to_hubspot,
                                                                    bq_to_dal_avoided_cost]




 