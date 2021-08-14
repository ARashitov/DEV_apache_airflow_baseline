from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
import global_configs
from direct_118 import local_env
from direct_118 import local_configs
from direct_118 import tasks
# from direct_118.common import REQUIREMENTS, default_args
# from direct_118.common import DIRECT_118_TAGS
# from direct_118.common import AIRFLOW_CONN_POSTGRES_CONTACT_DETAILS
# from direct_118.common import POPULAR_SEARCHES_TABLE
# from direct_118.common import BUSINESS_CATEGORIES_TABLE
# from direct_118.common import LOCATION_CATEGORY_URL_TABLE
# from direct_118.common import SEARCH_ENDPOINT
# from direct_118.common import LOCATIONS_2_SCRAPE
# from direct_118.tasks import factory_task_1
# from direct_118.tasks import factory_task_2
# from direct_118.tasks import factory_task_3


with DAG(dag_id='web_scrapping_direct118',
         default_args=global_configs.DEFAULT_ARGS,
         description=('Pipeline preparing web scrapping '
                      'paramters from direct 118'),
         schedule_interval=timedelta(days=1),
         start_date=days_ago(1),
         tags=local_configs.TAGS) as dag:

    t1_op_kwargs = {
        'postgres_uri': local_env.POSTGRES_URI,
        'target_stage_table': local_env.STAGE_TABLE_1,
    }
    t1 = tasks.factory_task_1(**t1_op_kwargs)


    t2_op_kwargs = {
        'postgres_uri': local_env.POSTGRES_URI,
        'source_stage_table': local_env.STAGE_TABLE_1,
        'target_stage_table': local_env.STAGE_TABLE_2,
        'requirements': local_configs.REQUIREMENTS
    }
    t2 = tasks.factory_task_2(**t2_op_kwargs)

    t3_op_kwargs = {
        'postgres_uri': local_env.POSTGRES_URI,
        'source_stage_table': local_env.STAGE_TABLE_2,
        'target_stage_table': local_env.STAGE_TABLE_3,
        'requirements': local_configs.REQUIREMENTS,
        'http_base_url': local_env.SEARCH_ENDPOINT,
        'locations': local_configs.LOCATIONS,
    }
    t3 = tasks.factory_task_3(**t3_op_kwargs)

    t4_op_kwargs = {
        'postgres_uri': local_env.POSTGRES_URI,
        'source_stage_table': local_env.STAGE_TABLE_3,
        'target_stage_table': local_env.STAGE_TABLE_4,
        'requirements': local_configs.REQUIREMENTS,
        'n_tasks': global_configs.N_PARALLEL_TASKS_MAX,
    }
    tasks_4 = tasks.factory_task_4(**t4_op_kwargs)

    t1 >> t2 >> t3
    for t4 in tasks_4:
        t3 >> t4