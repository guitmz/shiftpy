import uuid
import psycopg2
from const import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_BUCKET_NAME, \
    AWS_S3_REGION
from redshift.query import REDSHIFT_UPSERT_QUERY
from redshift.connection import run_query, redshift_conn


class RedshiftExporter(object):
    def __init__(self, s3path, table_name):
        s3info = s3path.split('/')
        self.s3folder = s3info[0]
        self.s3file = s3info[1]
        self.tmp_table = "temp_{}".format(uuid.uuid4().hex)
        self.table_name = table_name
        self.primary_keys = self.get_primary_keys()
        self.fields = self.get_fields()

    def get_primary_keys(self):
        sql = "SELECT indexdef " \
              "FROM pg_indexes " \
              "WHERE tablename = '{table_name}'"\
            .format(table_name=self.table_name)
        result = run_query(sql)[0][0]
        fields = result.split("(")[1].strip(")").split(",")
        return [x.strip().strip("\"") for x in fields]

    def get_fields(self):
        sql = "SELECT * FROM {table_name} LIMIT 0".format(
            table_name=self.table_name
        )
        try:
            with redshift_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    result = cur.description
                conn.commit()
        except psycopg2.Error as e:
            raise e
        table_fields = [item[0] for item in result]
        return table_fields

    def format_clause(self, clause):
        return ' AND '.join(
            [clause.format(
                pk=pk,
                tmp_table=self.tmp_table,
                table_name=self.table_name
            ) for pk in self.primary_keys]
        )

    def send_to_redshift(self):
        """
        This creates the necessary RedShift query, based on a single template.
        """

        where_clause = '{table_name}.{pk} = {tmp_table}.{pk}'
        insert_where_clause = '{tmp_table}.{pk} NOT IN ' \
                              '(SELECT {pk} FROM {table_name})'

        fields_insert = ",".join([k for k in self.fields])
        fields_update = ",\n".join("{column}={stg_table}.{column}".format(
            column=k,
            stg_table=self.tmp_table
            ) for k in self.fields
        )

        upsert_query = REDSHIFT_UPSERT_QUERY.format(
            s3_bucket=AWS_BUCKET_NAME,
            s3_region=AWS_S3_REGION,
            s3_access_key=AWS_ACCESS_KEY,
            s3_secret_key=AWS_SECRET_KEY,
            s3_folder=self.s3folder,
            s3_file=self.s3file,
            tmp_table=self.tmp_table,
            table_name=self.table_name,
            fields_insert=fields_insert,
            fields_update=fields_update,
            fields_where=self.format_clause(where_clause),
            fields_insert_where=self.format_clause(insert_where_clause)
        )

        run_query(upsert_query)
