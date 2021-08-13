"""
Author: Adil Rashitov
Created at: 13.08.2021
About:
    Scrapes business categories from popular searches
"""
import logging
from airflow.operators.python import PythonVirtualenvOperator


class Task2:

    @staticmethod
    def scrape_url(url: str):
        """
            Scrapes bussines title from URL

            Arguments:
            * url (str): URL address of source to scrape

            Returns:
            * (pd.DataFrame): List business categories
        """
        import cloudscraper
        from bs4 import BeautifulSoup
        import pandas as pd

        class_element = "popTermsList"
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            categories = soup \
                .find_all("ul", class_=class_element)[0] \
                .find_all("a")

            df = pd.DataFrame({
                'popular_searches': url,
                'business_category': list(map(lambda x: x.contents,
                                              categories)),
                'href': list(map(lambda x: x.attrs['href'], categories)),
            })
            df = df.explode('business_category').reset_index(drop=True)
            return df
        else:
            logging.error(f"Failure processing url: {url}")
            logging.error(f"Failure with status code: {response.status_code}")
            logging.error(f"Failure with text: {response.text}")

    @staticmethod
    def scrape_urls(args: dict) -> dict:
        """
            Scrapes from `url` category title

            Arguments:
            * _id (int): index of url in enumerated urls
            * url (str): URL to scrape from
            * export_stage_dir (str): file path to export
        """
        import pandas as pd

        dfs = []
        if len(args['urls']) > 0:
            for url in args['urls']:
                logging.info(f"Start scrapping: {url}")
                dfs.append(Task2.scrape_url(url))
                logging.info(f"Finish scrapping: {url}")

            dfs = pd.concat(dfs).reset_index(drop=True)
            args['data'] = dfs
        else:
            args['data'] = pd.DataFrame()
            logging.error("Nothing to scrape")
        return args

    @staticmethod
    def read_parameters(args: dict) -> dict:

        import pandas as pd

        df = pd.read_sql(args['select_template'],
                         args['postgres_connection'])
        args['urls'] = list(df['popular_searches'])
        if len(args['urls']) == 0:
            logging.error("No `popular_searches` are extracted")
        return args

    @staticmethod
    def export_table(args: dict) -> dict:
        if args['data'].shape[0] > 0:
            args['data'].to_sql(args['target_stage_table'],
                                con=args['postgres_connection'],
                                index=False, if_exists='replace')
        else:
            logging.error("Nothing to export")


def factory_task_2(postgres_uri: str,
                   source_stage_table: str,
                   target_stage_table: str,
                   requirements: list) -> PythonVirtualenvOperator:
    """
        factory function os second task

        Arguments:
        * postgres_uri (str): URI to postgres
        * source_stage_table (str): Table `popular_searches`
                                    categories will read
        * target_stage_table (str): Table business catgegories will be exported
        * requirements (list<str>): Python dependencies to install
                                    before execution of task

        Returns:
        * (PythonVirtualenvOperator): Apache airflow task
    """

    def task_2(postgres_uri: str,
               source_stage_table: str,
               target_stage_table: str):

        import logging
        from sqlalchemy import create_engine
        from direct_118.tasks.task_2 import Task2

        args = {
            'postgres_connection': create_engine(postgres_uri),
            'select_template': f'SELECT * FROM public."{source_stage_table}"',
            'target_stage_table': target_stage_table,
        }

        task_steps = [
            Task2.read_parameters,
            Task2.scrape_urls,
            Task2.export_table,
        ]

        for task_step in task_steps:
            logging.info(f"Start {task_step.__name__}")
            args = task_step(args)
            logging.info(f"Finish {task_step.__name__}\n")

    task = PythonVirtualenvOperator(
            task_id='export_business_categories',
            python_callable=task_2,
            op_kwargs={
                'postgres_uri': postgres_uri,
                'source_stage_table': source_stage_table,
                'target_stage_table': target_stage_table,
            },
            requirements=requirements,
        )

    return task
