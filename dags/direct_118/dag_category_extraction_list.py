import os
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from direct_118.extraction_business_categories_utils \
    import construct_urls_to_business_categ
from direct_118.config_common import DIRECT_118_TAGS


default_args = {
    'owner': 'Adil Rashitov',
    'depends_on_past': False,
    'email': ['adil.rashitov.98@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
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


with DAG(dag_id='prepare_business_category_extraction_list',
         default_args=default_args,
         description=('Preparation of list bussines categories to extract'
                      ' listed in direct 118'),
         schedule_interval=timedelta(days=1),
         start_date=days_ago(1),
         tags=DIRECT_118_TAGS) as dag:

    # 1. Building list of URLs with business categories letter
    stage1_file = os.environ['I_DIRECT_118_BUSINESS_CATEGORIES']

    t1 = PythonOperator(
        task_id='construct_parameters_to_get_available_categories',
        python_callable=construct_urls_to_business_categ,
        op_kwargs={'export_stage_fpath': stage1_file},
    )
