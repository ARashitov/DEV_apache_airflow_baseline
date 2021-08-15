"""
Author: Adil Rashitov
Created at: 14.08.2021
About:
    Performs webscrapping of contact details
"""
import pandas as pd


class ContactDetailsScrapper:

    def __slice_to_entities(self, bs, class_="result clearfix"):
        return bs.find_all('li', class_=class_)

    def __get_content(self, bs, elem, attrs):
        try:
            return bs.find(elem, attrs=attrs).contents[0]
        except Exception:
            return None

    def __get_href(self, bs, attrs):
        try:
            return bs.find('a', attrs=attrs).attrs['href']
        except Exception:
            return None

    def __get_element(self, bs, elem, attrs):
        try:
            return bs.find(elem, attrs=attrs)
        except Exception:
            return None

    def __get_email(self, bs):
        try:
            email = self.__get_element(bs, "li", {"class": "email-link"})
            return self.__get_href(email, {"class": "button"})
        except Exception:
            return None

    def __parse_business_entity(self, bs):
        return {
            "resultIndex": self.__get_content(bs, 'div',
                                              {"class": "resultIndex"}),
            "name": self.__get_content(bs, 'a', {"class": "name"}),
            "href": self.__get_href(bs, {"class": "name"}),
            "phone_number": self.__get_content(bs, 'span', {"class": "phone"}),
            "street_address": self.__get_content(bs, 'span', {"itemprop": "streetAddress"}),
            "address_locality": self.__get_content(bs, 'span', {"itemprop": "addressLocality"}),
            "postal_code": self.__get_content(bs, 'span', {"itemprop": "postalCode"}),
            "website": self.__get_href(bs, {"class": "button", "itemprop": "sameAs"}),
            "email": self.__get_email(bs)
        }

    def __replace_html_encodings(self, df):
        return df \
            .replace('y&#39;', "'", regex=True)

    def __separate_postal_code(self, df):
        df['postal_code'] = (df['postal_code'].str[:-3] + " - " +
                             df['postal_code'].str[-3:])
        return df

    def scrape(self, url: str) -> pd.DataFrame:
        from bs4 import BeautifulSoup
        import cloudscraper

        scrapper = cloudscraper.create_scraper()
        content = scrapper.get(url).text

        bs = BeautifulSoup(content)

        text = self.__slice_to_entities(bs)
        if len(text) == 0:
            text = self.__slice_to_entities(bs, 'result clearfix sponsored')
        if len(text) == 0:
            raise ValueError("Lack of entities to scrape")

        df = list(map(self.__parse_business_entity, text))

        df = pd.DataFrame(df)
        df['scrapping_url'] = url

        df = self.__replace_html_encodings(df)
        df = self.__separate_postal_code(df)

        return df


def task_6(postgres_uri: str, task_index: int, n_tasks: int,
           source_stage_table: str, target_stage_table: str):

    import sqlalchemy
    import logging
    from direct_118.tasks.task_6 import ContactDetailsScrapper

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
        import logging
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
        args['contact_details'] = scrapping_parameters
        return args

    def scrape_contact_details(args: dict) -> dict:
        """
            Extracts amount of business entities for
            specific category-location

            * args: dict with next fields:
                * what (str): Business category title
                * where (str): Location
                * url (str): Full url to extract locations
        """
        import logging
        import pandas as pd

        contact_details = ContactDetailsScrapper()
        output_dataframe = []

        for requests in args['contact_details'].to_dict('records'):
            output_dataframe.append(contact_details.scrape(requests['url']))
            logging.info(f"Finish webscrapping: {requests['url']}")

        args['contact_details'] = \
            pd.concat(output_dataframe).reset_index(drop=True)
        return args

    def export_table(args: dict) -> dict:
        import logging

        if args['contact_details'].shape[0] > 0:
            args['contact_details'].to_sql(args['target_stage_table'],
                                           con=args['postgres_connection'],
                                           index=False, if_exists='append')
            logging.info("Succesfull export to table:"
                         f" {args['target_stage_table']}")
            del args
        else:
            logging.error("Nothing to export")

    steps = [
        get_records_count,
        generate_select_page_statement,
        read_dataframe,
        scrape_contact_details,
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
