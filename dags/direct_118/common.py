import os
from datetime import timedelta

DIRECT_118_TAGS = ['Direct 118']


REQUIREMENTS = [
    "beautifulsoup4==4.9.3",
    "cloudscraper==1.2.58"
]


LOCATIONS_2_SCRAPE = [
    "Manchester",
    "Liverpool",
]

N_PARALLEL_TASKS_MAX = 20

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

# Direct 118 | General connections
SEARCH_ENDPOINT = os.environ['AIRFLOW_CONN_WWW_DIRECT_118']
AIRFLOW_CONN_POSTGRES = os.environ['AIRFLOW_CONN_POSTGRES']
AIRFLOW_CONN_POSTGRES_CONTACT_DETAILS = \
    os.environ['AIRFLOW_CONN_POSTGRES_CONTACT_DETAILS']

# DIRECT 118 | Staging tables
POPULAR_SEARCHES_TABLE = os.environ['I_DIRECT_118_POPULAR_SEARCHES_TABLE']
BUSINESS_CATEGORIES_TABLE = \
    os.environ['I_DIRECT_118_BUSINESS_CATEGORIES_TABLE']
LOCATION_CATEGORY_URL_TABLE = \
    os.environ['I_DIRECT_118_LOCATION_CATEGORY_URL_TABLE']
