from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from direct_118.common import REQUIREMENTS, default_args
from direct_118.common import DIRECT_118_TAGS
from direct_118.common import AIRFLOW_CONN_POSTGRES_CONTACT_DETAILS
from direct_118.common import POPULAR_SEARCHES_TABLE
from direct_118.common import BUSINESS_CATEGORIES_TABLE
from direct_118.common import LOCATION_CATEGORY_URL_TABLE
from direct_118.common import SEARCH_ENDPOINT
from direct_118.common import LOCATIONS_2_SCRAPE
from direct_118.tasks import factory_task_1
from direct_118.tasks import factory_task_2
from direct_118.tasks import factory_task_3


with DAG(dag_id='prepare_webscrapping_parameters',
         default_args=default_args,
         description=('Pipeline preparing web scrapping '
                      'paramters from direct 118'),
         schedule_interval=timedelta(days=1),
         start_date=days_ago(1),
         tags=DIRECT_118_TAGS) as dag:

    t1_op_kwargs = {
        'postgres_uri': AIRFLOW_CONN_POSTGRES_CONTACT_DETAILS,
        'output_table': POPULAR_SEARCHES_TABLE
    }
    t1 = factory_task_1(**t1_op_kwargs)

    t2_op_kwargs = {
        'postgres_uri': AIRFLOW_CONN_POSTGRES_CONTACT_DETAILS,
        'source_stage_table': POPULAR_SEARCHES_TABLE,
        'target_stage_table': BUSINESS_CATEGORIES_TABLE,
        'requirements': REQUIREMENTS
    }
    t2 = factory_task_2(**t2_op_kwargs)

    t3_op_kwargs = {
        'postgres_uri': AIRFLOW_CONN_POSTGRES_CONTACT_DETAILS,
        'source_stage_table': BUSINESS_CATEGORIES_TABLE,
        'target_stage_table': LOCATION_CATEGORY_URL_TABLE,
        'requirements': REQUIREMENTS,
        'http_base_url': SEARCH_ENDPOINT,
        'locations': LOCATIONS_2_SCRAPE,
    }
    t3 = factory_task_3(**t3_op_kwargs)

    t1 >> t2 >> t3
