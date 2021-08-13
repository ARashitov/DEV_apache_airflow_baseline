# import os
# import numpy as np
# import pandas as pd
# from airflow import DAG
# from pathlib import Path
# from datetime import timedelta
# from airflow.operators.python import PythonVirtualenvOperator
# from airflow.utils.dates import days_ago
# from direct_118.tasks import category_title_scrape
# from direct_118.tasks import export_1st_pages_at_loc_and_cat
# from direct_118.tasks import explode_urls_over_pages
# from direct_118.tasks import add_pages_as_params_to_urls
# from direct_118.config_common import DIRECT_118_TAGS
# from direct_118.config_common import REQUIREMENTS
# from direct_118.config_common import LOCATIONS_2_SCRAPE
# from direct_118.config_common import N_PARALLEL_TASKS_MAX


# default_args = {
#     'owner': 'Adil Rashitov',
#     'depends_on_past': False,
#     'email': ['adil.rashitov.98@gmail.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(seconds=20),
#     # 'queue': 'bash_queue',
#     # 'pool': 'backfill',
#     # 'priority_weight': 10,
#     # 'end_date': datetime(2016, 1, 1),
#     # 'wait_for_downstream': False,
#     # 'dag': dag,
#     # 'sla': timedelta(hours=2),
#     # 'execution_timeout': timedelta(seconds=300),
#     # 'on_failure_callback': some_function,
#     # 'on_success_callback': some_other_function,
#     # 'on_retry_callback': another_function,
#     # 'sla_miss_callback': yet_another_function,
#     # 'trigger_rule': 'all_success'
# }


# def generate_parallel_task1(urls_of_business_cats: list,
#                             export_stage_directory: str) -> list:
#     """
#         Generates task for each letter of business category

#         Arguments:
#         * urls_of_business_cats (list<str>): link of popularsearches
#         * export_stage_directory (str): Target directory to
#                                         export extracted categories

#         Returns:
#         * list<airflow.operators.python.PythonVirtualenvOperator>
#     """
#     url_index = 0
#     tasks = []
#     for url in urls_of_business_cats:
#         category_letter = url[-1]
#         tasks.append(
#             PythonVirtualenvOperator(
#                 task_id=f"get_business_categories_{category_letter}",
#                 python_callable=category_title_scrape,
#                 op_kwargs={'_id': url_index, 'url': url,
#                            'export_stage_dir': export_stage_directory},
#                 system_site_packages=True,
#                 requirements=REQUIREMENTS,
#             ))
#         url_index += 1
#     return tasks


# def generate_parallel_task3(source_stage_file: str,
#                             target_stage_dir: str) -> list:
#     """
#         Generates task for each letter of business category

#         Arguments:
#         * source_stage_file (str): Source file to explode URL pages over
#         * target_stage_dir (str): Path to directory
#                                   where processes will export their tasks

#         Returns:
#         * list<airflow.operators.python.PythonVirtualenvOperator>
#     """
#     args = pd.read_csv(source_stage_file).to_dict('records')
#     args = np.array_split(args, N_PARALLEL_TASKS_MAX)
#     task_index = list(range(N_PARALLEL_TASKS_MAX))

#     def generate_tasks(task_index: int, args: args):
#         """
#             Generates task for each process

#             Arguments:
#             * _id (int): Task index

#         """
#         export_fpath = f"{target_stage_dir}{task_index}.csv.zip"
#         return PythonVirtualenvOperator(
#                 task_id=f"get_n_entities_{task_index}",
#                 python_callable=explode_urls_over_pages,
#                 op_kwargs={'args': args,
#                            'export_fpath': export_fpath},
#                 system_site_packages=True,
#                 requirements=REQUIREMENTS,
#             )

#     return list(map(generate_tasks,
#                     task_index, args))


# with DAG(dag_id='generate_webscrapping_parameters',
#          default_args=default_args,
#          description=('Webscrapping of business categories available'
#                       ' in direct 118'),
#          schedule_interval=timedelta(days=1),
#          start_date=days_ago(1),
#          tags=DIRECT_118_TAGS) as dag:

#     # 1. Listing staging directories & reading configs
#     contact_details_endpoint = os.environ['AIRFLOW_CONN_WWW_DIRECT_118']
#     stage1_file = os.environ['I_DIRECT_118_BUSINESS_CATEGORIES']
#     stage2_dir = os.environ['I_DIRECT_118_SCRAPPED_CATEGORIES_DIR']
#     stage3_file = os.environ['I_DIRECT_118_FIRST_PAGE_REQUESTS']
#     stage4_dir = os.environ['I_DIRECT_118_N_PAGES_STAGE_DIR']
#     stage5_file = os.environ['O_DIRECT_118_WEB_SCRAPPING_PARAMETER']

#     # 1.1 Ensure stage folders presence
#     Path(stage2_dir).mkdir(parents=True, exist_ok=True)
#     Path(stage4_dir).mkdir(parents=True, exist_ok=True)

#     urls_of_business_cats = list(pd.read_csv(stage1_file)['urls'])

#     # 2. Scrapping of URLs
#     tasks_1 = generate_parallel_task1(
#         urls_of_business_cats=urls_of_business_cats,
#         export_stage_directory=stage2_dir)

#     # 3. Building first requests for specific category and location
#     t2 = PythonVirtualenvOperator(
#         task_id="export_1st_pages_at_loc_and_cat",
#         python_callable=export_1st_pages_at_loc_and_cat,
#         op_kwargs={'http_base': contact_details_endpoint,
#                    'categories_stage_dir': stage2_dir,
#                    'locations': LOCATIONS_2_SCRAPE,
#                    'export_fpath': stage3_file},
#         system_site_packages=True,
#         requirements=REQUIREMENTS,
#     )

#     tasks_3 = generate_parallel_task3(source_stage_file=stage3_file,
#                                       target_stage_dir=stage4_dir)

#     t4 = PythonVirtualenvOperator(
#         task_id="add_pages_as_params_to_urls",
#         python_callable=add_pages_as_params_to_urls,
#         op_kwargs={'stage_dir_fpath': stage4_dir,
#                    'stage_export_file': stage5_file},
#         system_site_packages=True,
#     )

#     # 3. Sequence specification
#     for t1 in tasks_1:
#         t1 >> t2

#     for t3 in tasks_3:
#         t2 >> t3
#         t3 >> t4
