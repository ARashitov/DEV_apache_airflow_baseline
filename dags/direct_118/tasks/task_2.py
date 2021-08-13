from airflow.operators.python import PythonVirtualenvOperator


def factory_task_2(postgres_uri: str,
                   source_stage_table: str,
                   target_stage_table: str,
                   requirements: list) -> PythonVirtualenvOperator:

    def category_title_scrape(postgres_uri: str, source_stage_table: str,
                              target_stage_table: str):
        """
            Scrapes from `url` category title

            Arguments:
            * _id (int): index of url in enumerated urls
            * url (str): URL to scrape from
            * export_stage_dir (str): file path to export
        """
        import logging
        import pandas as pd
        import cloudscraper
        from bs4 import BeautifulSoup
        from sqlalchemy import create_engine

        ulrs = pd.read_sql(sql=f'SELECT * FROM public."{source_stage_table}"',
                        con=create_engine(postgres_uri))
        urls = urls['popular_searches']

        def scrape_url(url: str):
            """
                Scrapes bussines title from URL

                Arguments:
                * url (str): URL address of source to scrape

                Returns:
                * (pd.DataFrame): List business categories
            """

            class_element = "popTermsList"

            logging.info(f"Start processing url: {url}")
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                categories = soup \
                    .find_all("ul", class_=class_element)[0] \
                    .find_all("a")

                df = pd.DataFrame({
                    'url': url,
                    'business_category': list(map(lambda x: x.contents,
                                                categories)),
                    'href': list(map(lambda x: x.attrs['href'], categories)),
                })
                df = df.explode('business_category').reset_index(drop=True)
                logging.info(f"Finish processing url: {url}")
                return df
            else:
                logging.error(f"Failure processing url: {url}")
                logging.error(f"Failure with status code: {response.status_code}")
                logging.error(f"Failure with text: {response.text}")

    t2 = PythonVirtualenvOperator(
        task_id=f"scrapping_categories",
        python_callable=category_title_scrape,
        op_kwargs={'postgres_uri': postgres_uri,
                   'source_stage_table': source_stage_table},
        system_site_packages=True,
        requirements=requirements)

    return t2
