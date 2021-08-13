def create_database(source_db_uri: str, target_db_uri: str):
    """ query data from the vendors table """
    import logging
    import psycopg2

    conn = None
    try:
        target_db = target_db_uri.rsplit('/', 1)[1]
        conn = psycopg2.connect(source_db_uri)
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {target_db}")
        logging.info(f"Successfull create db : {target_db}")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"{error}")
    finally:
        if conn is not None:
            conn.close()
