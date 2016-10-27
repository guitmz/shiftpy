REDSHIFT_UPSERT_QUERY = """
CREATE TEMPORARY TABLE {tmp_table} (LIKE {table_name});

COPY {tmp_table} (
        {fields_insert}
)
FROM 's3://{s3_bucket}/redshift/{s3_folder}/{s3_file}'
region '{s3_region}'
CREDENTIALS 'aws_access_key_id={s3_access_key};aws_secret_access_key={s3_secret_key}'
COMPUPDATE OFF
STATUPDATE OFF
TRUNCATECOLUMNS
TRIMBLANKS
DELIMITER ','
CSV QUOTE as '"'
EMPTYASNULL
ACCEPTANYDATE
DATEFORMAT as 'auto'
GZIP
;

BEGIN;
LOCK {table_name}
UPDATE {table_name}
SET

{fields_update}

FROM {tmp_table}
WHERE
    {fields_where};

INSERT INTO {table_name} (
        {fields_insert}
)
SELECT DISTINCT {fields_insert}
FROM {tmp_table}
WHERE {fields_insert_where};
END;
"""