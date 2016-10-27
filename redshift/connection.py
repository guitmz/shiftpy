from const import REDSHIFT_PASSWORD, REDSHIFT_USERNAME, REDSHIFT_URL, REDSHIFT_DATABASE, REDSHIFT_PORT
import psycopg2


def redshift_conn():
    conn_str = \
        "dbname='{db_name}' user='{db_user}' " \
        "password='{db_pass}' port='{db_port}' " \
        "host='{db_host}'".format(
            db_name=REDSHIFT_DATABASE,
            db_user=REDSHIFT_USERNAME,
            db_pass=REDSHIFT_PASSWORD,
            db_port=REDSHIFT_PORT,
            db_host=REDSHIFT_URL,
        )
    return psycopg2.connect(conn_str)


def run_query(query):
    try:
        with redshift_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()
            conn.commit()
    except psycopg2.Error as e:
        raise e
    return result
