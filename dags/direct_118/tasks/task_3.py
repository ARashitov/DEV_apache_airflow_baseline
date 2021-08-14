"""
Author: Adil Rashitov
Created at: 13.08.2021
About:
    Generates first urls to contact details
"""
from airflow.operators.python import PythonVirtualenvOperator


class Task3:

    @staticmethod
    def read_categories(args):

        import pandas as pd
        import logging

        args['data'] = pd.read_sql(args['select_statement'],
                                   con=args['postgres_connection'])
        if args['data'].shape[0] == 0:
            logging.info("Nothing to read")
        return args

    @staticmethod
    def cartesian_product_location_and_category(args):

        import pandas as pd
        from itertools import product

        # 1. Extraction arguments
        categories = args['data']['business_category']
        locations = args['locations']

        # 2. Cartesion product
        permutations = list(product(categories, locations))
        df = pd.DataFrame(permutations, columns=['what', 'where'])
        args['data'] = df

        return args

    @staticmethod
    def update_url(args):
        df = args['data']
        df['url'] = (args['http_base_url'] + 'what=' +
                     df['what'] + '&where=' + df['where'])
        args['data'] = df
        return args

    @staticmethod
    def export_table(args: dict) -> dict:
        import logging

        if args['data'].shape[0] > 0:
            args['data'].to_sql(args['target_stage_table'],
                                con=args['postgres_connection'],
                                index=False, if_exists='replace')
        else:
            logging.error("Nothing to export")


def factory_task_3(postgres_uri: str,
                   source_stage_table: str,
                   target_stage_table: str,
                   http_base_url: str,
                   locations: list,
                   requirements: list,):

    def task_3(postgres_uri: str,
               source_stage_table: str,
               target_stage_table: str,
               http_base_url: str,
               locations: list):

        import logging
        import sqlalchemy
        from direct_118.tasks.task_3 import Task3

        args = {
            'postgres_connection': sqlalchemy.create_engine(postgres_uri),
            'select_statement': f'SELECT * FROM public."{source_stage_table}"',
            'locations': locations,
            'http_base_url': http_base_url,
            'target_stage_table': target_stage_table,
        }

        task_steps = [
            Task3.read_categories,
            Task3.cartesian_product_location_and_category,
            Task3.update_url,
            Task3.export_table,
        ]

        for step in task_steps:
            logging.info(f"Start {step.__name__}")
            args = step(args)
            logging.info(f"Finish {step.__name__}\n")
        del args

    task = PythonVirtualenvOperator(
            task_id='export_locaiton_category_url',
            python_callable=task_3,
            op_kwargs={
                'postgres_uri': postgres_uri,
                'source_stage_table': source_stage_table,
                'target_stage_table': target_stage_table,
                'http_base_url': http_base_url,
                'locations': locations,
            },
            requirements=requirements,
        )

    return task
