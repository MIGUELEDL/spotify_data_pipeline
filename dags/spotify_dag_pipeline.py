from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import importlib.util, sys

#____________________________________________________________________________________________

# Funções que as tasks vão executar

def extract():
    print("buscando dados...")
    spec = importlib.util.spec_from_file_location("extract", "/opt/airflow/scripts/extract.py")
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

def transform():
    print("processando dados...")
    spec = importlib.util.spec_from_file_location("transform", "/opt/airflow/scripts/transform.py")
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

def load():
    print("salvando dados...")
    spec = importlib.util.spec_from_file_location(
        "load",
        "/opt/airflow/scripts/load.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.run_gold_pipeline()

def falha(context):
    print(f"FALHOU: {context['task_instance'].task_id}")

#____________________________________________________________________________________________

# Configurações padrão de todas as tasks

default_args = {
    "owner": "miguel",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": falha,
}

# A DAG

with DAG(
    dag_id="spotify_data_pipeline",  # nome da dag
    description="dag pipeline do projeto spotify_data_pipeline",
    default_args=default_args,  
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",      # todo dia à 00:00h
    catchup=False,                   # não reprocessa os dados do passado
    max_active_runs=1,               # só roda 1 processo por vez
) as dag:

    # Tasks

    # inicio do pipeline
    inicio = EmptyOperator(task_id="inicio")

    # extração
    extract = PythonOperator(
        task_id="extract",    
        python_callable=extract,
    )

    # transformação
    transform = PythonOperator(
        task_id="transform",  
        python_callable=transform
    )

    # carregamento
    load = PythonOperator(
        task_id="load",       
        python_callable=load
    )

    fim_ok  = EmptyOperator(task_id="fim_ok",   trigger_rule=TriggerRule.ALL_SUCCESS) # se der sucesso mostra
    fim_err = EmptyOperator(task_id="fim_erro", trigger_rule=TriggerRule.ONE_FAILED)  # se falhar mostra a falha

    # Ordem de execução
    inicio >> extract >> transform >> load >> [fim_ok, fim_err]