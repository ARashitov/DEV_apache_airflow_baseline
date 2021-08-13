from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from direct_118.common import default_args
from direct_118.common import DIRECT_118_TAGS
from direct_118.common import AIRFLOW_CONN_POSTGRES_CONTACT_DETAILS
from direct_118.common import POPULAR_SEARCHES_TABLE
from direct_118.tasks import factory_task_1


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
