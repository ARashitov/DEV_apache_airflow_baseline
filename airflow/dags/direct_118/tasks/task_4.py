"""
Author: Adil Rashitov
Created at: 14.08.2021
About:
    Performs webscrapping of n_entites to add pages parameter
"""


def task_4(task_index: str,
           n_tasks: int,
           postgres_uri: str,
           source_stage_table: str,
           target_stage_table: str):
    """
        Adds `page` parameters to urls with taking
        into account amount of entities need to be extracted

        Arguments:
        * select_page_statement (str): Select statement to read
                                    specific page (LIMIT + OFFSET)
        * postgres_uri (str): URI to postgres
        * target_stage_table (str): Table with webscrapping parameters
    """
    import logging
    import sqlalchemy
    import pandas as pd

    def get_records_count(args: dict) -> dict:
        """
            Selects amount records from source table and
            stores to `n_records`
        """
        import pandas as pd
        sql = f'SELECT COUNT(*) from public."{args["source_stage_table"]}"'
        args['n_records'] = pd.read_sql(sql,
                                        con=args['postgres_connection'])
        args['n_records'] = args['n_records']['count'][0]
        return args

    def generate_select_page_statement(args: dict) -> dict:
        """
            Function performs mapping os page
            to task_index using ofsetting and limiting.
        """
        limit = args['n_records'] // args['n_tasks'] + 1
        offset = args['task_index'] * limit
        args['select_statement'] = \
            (f'SELECT * FROM public."{args["source_stage_table"]}"' +
             f" OFFSET {offset} LIMIT {limit}")
        logging.info(f"SELECT_STATEMENT: {args['select_statement']}")
        return args

    def read_dataframe(args: dict) -> dict:
        """
            Consolidates all chunks of business categories title
            to a single pandas DataFrame and retuns them
            as a single pandas Series
        """
        import logging
        import pandas as pd

        scrapping_parameters = pd.read_sql(args['select_statement'],
                                           con=args['postgres_connection'])
        if scrapping_parameters.shape[0] == 0:
            logging.error("Nothing to read...")
        args['scrapping_params'] = scrapping_parameters
        return args

    def scrape_amount_of_business_entities(args: dict) -> dict:
        """
            Extracts amount of business entities for
            specific category-location

            * args: dict with next fields:
                * what (str): Business category title
                * where (str): Location
                * url (str): Full url to extract locations
        """
        import re
        import logging
        import pandas as pd
        import cloudscraper
        from bs4 import BeautifulSoup

        scrapper = cloudscraper.create_scraper()
        output_dataframe = []

        for params in args['scrapping_params'].to_dict('records'):
            response = scrapper.get(params['url'])

            if response.status_code == 200:

                try:

                    # 1. Extraction amount of entities for
                    #    specific category & location
                    soup = BeautifulSoup(response.text, "html.parser")
                    n_entities = soup \
                        .find_all("div", class_="resultInfoBlock")[0] \
                        .contents[0]

                    # 2. Finding numeric values using regex
                    n_entities = re.findall(r'\d+', n_entities.split('of ')[1])
                    params['n_entities'] = int(n_entities[0])
                    logging.info(f"Succesfull finish: {params['url']}")

                except Exception as exc:

                    logging.error(f"Failure finish {params['url']}: "
                                  f"{str(exc)}")
                    params['n_entities'] = 0

                output_dataframe.append(params)

            else:

                logging.error(f"Error at GET (status code): "
                              f"{response.status_code}")
                logging.error(f"Error at GET (text): {response.text}")

        args['scrapping_params'] = pd.DataFrame(output_dataframe)
        return args

    def compute_page_count_for_each_request(args: dict) -> dict:
        """
            Computes amount of pages needed to extract all contact
            details for specific location-category
        """
        scrapping_params = args['scrapping_params']
        entities_per_list = 15

        n_int_pages = scrapping_params['n_entities'] // entities_per_list
        fraction = (scrapping_params['n_entities'] % entities_per_list) > 0
        scrapping_params['n_pages'] = n_int_pages + fraction
        args['scrapping_params'] = scrapping_params

        return args

    def add_page_number_parameter(args: dict) -> dict:
        """
            Generates page number
        """
        def generate_url_with_pages(arg):
            if arg['n_pages'] > 0:
                page = pd.Series(range(1, arg['n_pages']+1, 1)).astype(str)
                output = pd.DataFrame({
                    **arg,
                    'page': page
                })
            else:
                output = pd.DataFrame()
            return output

        dfs = list(map(
            generate_url_with_pages,
            args['scrapping_params'].to_dict('records')
        ))
        dfs = pd.concat(dfs).reset_index(drop=True)
        args['scrapping_params'] = dfs
        return args

    def update_url(args: dict) -> dict:
        dfs = args['scrapping_params']
        dfs['url'] = dfs['url'] + "&page=" + dfs['page']
        args['scrapping_params'] = dfs
        return args

    def slice_dataframe(args: dict) -> dict:
        scrapping_params = args['scrapping_params'][['what', 'where', 'url']]
        args['scrapping_params'] = scrapping_params
        return args

    def export_table(args: dict) -> dict:
        import logging

        if args['scrapping_params'].shape[0] > 0:
            args['scrapping_params'].to_sql(args['target_stage_table'],
                                            con=args['postgres_connection'],
                                            index=False, if_exists='append')
        else:
            logging.error("Nothing to export")

    steps = [
        # 1. Mapping select page to `task_index`
        get_records_count,
        generate_select_page_statement,
        # 2. Reading & scrapping amount of entites
        #    from each location & category (url)
        read_dataframe,
        scrape_amount_of_business_entities,
        # 3. Computes amount of pages based on amount of entities
        compute_page_count_for_each_request,
        # 4. add page parameters
        add_page_number_parameter,
        # 5. Update URL
        update_url,
        slice_dataframe,
        export_table,
    ]

    args = {
        'postgres_connection': sqlalchemy.create_engine(postgres_uri),
        'task_index': task_index,
        'n_tasks': n_tasks,
        'source_stage_table': source_stage_table,
        'target_stage_table': target_stage_table,
    }

    for step in steps:
        logging.info(f"Start: {step.__name__}()")
        args = step(args)
        logging.info(f"Finish: {step.__name__}()\n")
    return args
