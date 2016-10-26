from const import REDSHIFT_PASSWORD, REDSHIFT_USERNAME, REDSHIFT_URL, REDSHIFT_DATABASE
import psycopg2


def redshift_conn():
    conn_str = \
        "dbname='{db_name}' user='{db_user}' " \
        "password='{db_pass}' port='{db_port}' " \
        "host='{db_host}'".format(
            db_name=REDSHIFT_DATABASE,
            db_user=REDSHIFT_USERNAME,
            db_pass=REDSHIFT_PASSWORD,
            db_port=5439,
            db_host=REDSHIFT_URL,
        )
    return psycopg2.connect(conn_str)


def redshift_run_query(query):
    try:
        with redshift_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        raise 'Error {error_code} running query: {error_msg}'.format(
            error_code=e.pgcode, error_msg=e.pgerror
        )
