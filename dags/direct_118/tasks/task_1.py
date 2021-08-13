from airflow.operators.python import PythonOperator


def factory_task_1(postgres_uri: str, output_table: str) -> PythonOperator:

    from airflow.operators.python import PythonOperator

    def export_categories_listed_by_letters(postgres_uri: str,
                                            output_table: str) -> None:
        """
            Function exports to pandas dataframe urls
            of available business categories in direct 118
            starting with specific letter from a-z.
        """
        import string
        import logging
        import pandas as pd
        from sqlalchemy import create_engine

        # 1. Construction pandas dataframe of 118 business categories
        URL = "http://www.118.direct/popularsearches/"
        business_categs = pd.Series(map(lambda x: f"{URL}{x}",
                                        list(string.ascii_lowercase)))
        df = pd.DataFrame({
            'popular_searches': business_categs
        })

        # 2. Export
        df.to_sql(name=output_table,
                  con=create_engine(postgres_uri),
                  index=False, if_exists='replace')
        logging.info(f"Popular searches are exported to: {output_table}")

    t1 = PythonOperator(
            task_id='export_categories_listed_by_letters',
            python_callable=export_categories_listed_by_letters,
            op_kwargs={
                'postgres_uri': postgres_uri,
                'output_table': output_table,
            }
        )

    return t1
