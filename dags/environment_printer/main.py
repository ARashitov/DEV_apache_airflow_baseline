import os
import sys
sys.path.append(os.getcwd().rsplit('/', 1)[0])
from common_configs import implemented_dag_tags
import logging
from datetime import timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Adil Rashitov',
    'depends_on_past': False,
    'email': ['adil.rashitov.98@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


def print_env_vars():
    for key, value in os.environ.items():
        logging.info(f"     [{key}]: {value}")

def print_current_dir():
    logging.info(f"[CURRENT DIRECTORY]: {os.getcwd()}")
    logging.info(f"[DAG TAG]: {implemented_dag_tags}")


with DAG(
    'test-enviornment-vars',
    default_args=default_args,
    description='A simple test of presence environment variables specified in .env file.',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=implemented_dag_tags) as dag:

    t1 = PythonOperator(
        task_id='print_env_vars',
        python_callable=print_env_vars,
    )

    t2 = PythonOperator(
        task_id='print_current_dir_dag',
        python_callable=print_current_dir,
    )

    t1 >> t2
