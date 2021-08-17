from airflow.providers.postgres.operators.postgres import PostgresOperator


def task_7(postgres_uri: str, tables_to_drop: list):

    from functools import reduce

    def factory_drop_statmenet(table):
        return f'DROP TABLE IF EXISTS public."{table}" CASCADE;'

    drop_statements = [
       factory_drop_statmenet(table) for table in tables_to_drop
    ]

    drop_statement = reduce(lambda x, y: f'{x}\n{y}', drop_statements)

    task = PostgresOperator(
        task_id="drop_staging_tables",
        postgres_conn_id=postgres_uri,
        sql=drop_statement,
    )

    return task
