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
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(dag_id='web_scrapping_direct118',
         default_args=global_configs.DEFAULT_ARGS,
         description=('Web scrapping contact details from Direct118'),
         schedule_interval=timedelta(days=1),
         start_date=days_ago(1),
         tags=local_configs.TAGS) as dag:

    def build_sql(tables: list) -> str:
        """
            Collection for drop tables
        """
        SQL = "DROP TABLE IF EXISTS "
        for table in tables:
            SQL += f' public."{table}",'
        SQL = SQL[:-1]
        SQL += ';'
        return SQL

    t0 = PostgresOperator(
        task_id="drop_staging_tables_before_execution",
        postgres_conn_id='postgres_contact_details',
        sql=build_sql([
            local_env.STAGE_TABLE_1,
            local_env.STAGE_TABLE_2,
            local_env.STAGE_TABLE_3,
            local_env.STAGE_TABLE_4,
        ]),
    )

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

    t5 = generators \
        .get_sequential_task(AirflowOperator=DummyOperator,
                             operator_args={'task_id': 'tasks_glue_operator'})

    t6_op_kwargs = {
        'postgres_uri': local_env.POSTGRES_URI,
        'source_stage_table': local_env.STAGE_TABLE_4,
        'target_stage_table': local_env.OUTPUT_TABLE,
    }
    t6_operator_args = {
        'task_id': 'web_scrapping_contact_details',
        'python_callable': tasks.task_6,
        'requirements': local_configs.REQUIREMENTS,
    }
    tasks_6 = generators \
        .get_parallel_task(AirflowOperator=PythonVirtualenvOperator,
                           operator_args=t6_operator_args,
                           n_tasks=global_configs.N_PARALLEL_TASKS_MAX,
                           op_kwargs=t6_op_kwargs)

    t7 = PostgresOperator(
        task_id="drop_staging_tables_after_execution",
        postgres_conn_id='postgres_contact_details',
        sql=build_sql([
            local_env.STAGE_TABLE_1,
            local_env.STAGE_TABLE_2,
            local_env.STAGE_TABLE_3,
            local_env.STAGE_TABLE_4,
        ]),
    )

    t0 >> t1 >> t2 >> t3
    for t4 in tasks_4:
        t3 >> t4 >> t5
    for t6 in tasks_6:
        t5 >> t6 >> t7
