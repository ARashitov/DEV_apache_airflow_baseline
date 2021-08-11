import os
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from direct_118.scrapping_categs_titles_utils import scrape_titles
from direct_118.generation_search_params_utils \
    import generate_search_parameters
from direct_118.config_common import DIRECT_118_TAGS
from direct_118.config_common import REQUIREMENTS
from direct_118.config_common import LOCATIONS


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


def generate_parallel_task(task_prefix: str,
                           args: dict,
                           python_callable: callable) -> list:

    python_operators = []
    for op_kwargs in args:
        letter = op_kwargs['url'][-1]
        python_operators.append(
            PythonVirtualenvOperator(
                task_id=f"{task_prefix}_{letter}",
                python_callable=python_callable,
                op_kwargs=op_kwargs,
                system_site_packages=True,
                requirements=REQUIREMENTS,
            ))
    return python_operators


with DAG(dag_id='prepare_webscrapping_parameters',
         default_args=default_args,
         description=('Webscrapping of business categories available'
                      ' in direct 118'),
         schedule_interval=timedelta(days=1),
         start_date=days_ago(1),
         tags=DIRECT_118_TAGS) as dag:

    # 1. Listing staging directories
    stage1_file = os.environ['I_DIRECT_118_BUSINESS_CATEGORIES']
    stage2_dir = os.environ['I_DIRECT_118_SCRAPPED_CATEGORIES_DIR']
    stage3_file = os.environ['I_DIRECT_118_FIRST_PAGE_REQUESTS']

    # 2. Scrapping of URLs
    _id = 0
    tasks_1 = []
    for url in list(pd.read_csv(stage1_file)['business_categs']):
        category_letter = url[-1]
        tasks_1.append(
            PythonVirtualenvOperator(
                task_id=f"get_available_business_categories_{category_letter}",
                python_callable=scrape_titles,
                op_kwargs={'_id': _id, 'url': url,
                           'export_stage_dir': stage2_dir},
                system_site_packages=True,
                requirements=REQUIREMENTS,
            ))
        _id += 1

    # 3. Building first requests for specific category and location
    t2 = PythonVirtualenvOperator(
        task_id="first_requests_of_specific_category_and_location",
        python_callable=generate_search_parameters,
        op_kwargs={'http_base': os.environ['AIRFLOW_CONN_WWW_DIRECT_118'],
                   'categories_stage_dir': stage2_dir,
                   'locations': LOCATIONS,
                   'export_fpath': stage3_file},
        system_site_packages=True,
        requirements=REQUIREMENTS,
    )

    # 3. Sequence specification
    for t1 in tasks_1:
        t1 >> t2
