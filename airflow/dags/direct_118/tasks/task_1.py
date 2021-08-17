"""
Author: Adil Rashitov
Created at: 13.08.2021
About:
    Generates url to `popular_searches` where business categories are listed
"""


def task_1(postgres_uri: str, target_stage_table: str) -> None:
    """
        Function exports to pandas dataframe urls
        of available business categories in direct 118
        starting with specific letter from a-z.

        Arguments:
        * postgres_uri (str): URI to postgres
        * target_stage_table (str):
    """
    import string
    import logging
    import pandas as pd
    from sqlalchemy import create_engine

    # 1. Construction pandas dataframe of 118 business categories
    URL = "http://www.118.direct/popularsearches/"
    popular_searches = pd.Series(map(lambda x: f"{URL}{x}",
                                     list(string.ascii_lowercase)))
    df = pd.DataFrame({
        'popular_searches': popular_searches
    })

    # 2. Export
    df.to_sql(name=target_stage_table,
              con=create_engine(postgres_uri),
              index=False, if_exists='replace')
    logging.info(f"Popular searches are exported to: {target_stage_table}")
