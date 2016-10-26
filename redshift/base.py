import uuid
from const import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_BUCKET_NAME, AWS_S3_REGION
from redshift.query import REDSHIFT_UPSERT_QUERY
from redshift.connection import redshift_run_query


class RedshiftExporter(object):
    def __init__(self, s3path, table_name, fields):
        s3info = s3path.split('/')
        self.s3folder = s3info[0]
        self.s3file = s3info[1]
        self.table_name = table_name
        self.fields = fields

    def send_to_redshift(self):
        """
        This creates the necessary RedShift query, based on a single template.
        """
        tmp_table = 'temp_{}'.format(uuid.uuid4().hex)
        fields_insert = ','.join([k for k in self.fields])
        fields_update = ',\n'.join('{column}={stg_table}.{column}'.format(
            column=k,
            stg_table=tmp_table
            ) for k in self.fields
        )

        upsert_query = REDSHIFT_UPSERT_QUERY.format(
            s3_bucket=AWS_BUCKET_NAME,
            s3_region=AWS_S3_REGION,
            s3_access_key=AWS_ACCESS_KEY,
            s3_secret_key=AWS_SECRET_KEY,
            s3_folder=self.s3folder,
            s3_file=self.s3file,
            tmp_table=tmp_table,
            table_name=self.table_name,
            fields_insert=fields_insert,
            fields_update=fields_update
        )

        redshift_run_query(upsert_query)
