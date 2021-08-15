from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
import global_configs
import generators
from direct_118 import local_env
from direct_118 import local_configs
from direct_118 import tasks
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.python import PythonOperator


with DAG(dag_id='web_scrapping_direct118',
         default_args=global_configs.DEFAULT_ARGS,
         description=('Web scrapping contact details from Direct118'),
         schedule_interval=timedelta(days=1),
         start_date=days_ago(1),
         tags=local_configs.TAGS) as dag:

    t1_op_kwargs = {
        'postgres_uri': local_env.POSTGRES_URI,
        'target_stage_table': local_env.STAGE_TABLE_1,
    }
    t1_operator_args = {
        'task_id': 'export_popluar_searches_urls',
        'python_callable': tasks.task_1,
    }
    t1 = generators.get_sequential_task(AirflowOperator=PythonOperator,
                                        operator_args=t1_operator_args,
                                        op_kwargs=t1_op_kwargs)

    t2_op_kwargs = {
        'postgres_uri': local_env.POSTGRES_URI,
        'source_stage_table': local_env.STAGE_TABLE_1,
        'target_stage_table': local_env.STAGE_TABLE_2,
        'requirements': local_configs.REQUIREMENTS
    }
    t2_operator_args = {
        'task_id': 'export_business_categories',
        'python_callable': tasks.task_2,
        'requirements': local_configs.REQUIREMENTS,
    }
    t2 = generators \
        .get_sequential_task(AirflowOperator=PythonVirtualenvOperator,
                             operator_args=t2_operator_args,
                             op_kwargs=t2_op_kwargs)

    t3_op_kwargs = {
        'postgres_uri': local_env.POSTGRES_URI,
        'source_stage_table': local_env.STAGE_TABLE_2,
        'target_stage_table': local_env.STAGE_TABLE_3,
        'http_base_url': local_env.SEARCH_ENDPOINT,
        'locations': local_configs.LOCATIONS,
    }
    t3_operator_args = {
        'task_id': 'export_locaiton_category_url',
        'python_callable': tasks.task_3,
        'requirements': local_configs.REQUIREMENTS,
    }
    t3 = generators \
        .get_sequential_task(AirflowOperator=PythonVirtualenvOperator,
                             operator_args=t3_operator_args,
                             op_kwargs=t3_op_kwargs)

    t4_op_kwargs = {
        'postgres_uri': local_env.POSTGRES_URI,
        'source_stage_table': local_env.STAGE_TABLE_3,
        'target_stage_table': local_env.STAGE_TABLE_4,
    }
    t4_operator_args = {
        'task_id': 'export_locaiton_category_url',
        'python_callable': tasks.task_4,
        'requirements': local_configs.REQUIREMENTS,
    }
    tasks_4 = generators \
        .get_parallel_task(AirflowOperator=PythonVirtualenvOperator,
                           operator_args=t4_operator_args,
                           n_tasks=global_configs.N_PARALLEL_TASKS_MAX,
                           op_kwargs=t4_op_kwargs)

    t1 >> t2 >> t3
    for t4 in tasks_4:
        t3 >> t4
