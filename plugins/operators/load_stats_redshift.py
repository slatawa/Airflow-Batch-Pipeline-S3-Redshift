from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import tempfile
import pandas as pd
import numpy as np


class LoadStatsRedshift(BaseOperator):

    load_search_stats_sql = """
            INSERT INTO {} {} VALUES {}
        """

    def __init__(self, aws_conn_id, aws_destination_bucket, aws_dest_key ,postgres_conn_id, table, *args, **kwargs):
        super(LoadStatsRedshift, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.aws_destination_bucket = aws_destination_bucket
        self.aws_dest_key = aws_dest_key
        self.aws_conn_id = aws_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.table = table

    def execute(self, context):
        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        redshift_hook = PostgresHook(self.postgres_conn_id)
        source_file_obj = hook.get_key(bucket_name=self.aws_destination_bucket, key=self.aws_dest_key.format(**context))
        tf = tempfile.NamedTemporaryFile()
        tmp_file_name = tf.name
        source_file_obj.download_file(tmp_file_name)
        df = pd.read_csv(tmp_file_name, header=0, sep=',', quotechar='"')
        day = '{ds_nodash}'.format(**context)
        num_searches = np.sum(df['num_eligible_searches'])
        num_users = len(df)
        num_rental_searches = np.sum(df['num_rental_search_type'])
        num_sale_searches = np.sum(df['num_sale_search_type'])
        num_none_searches = np.sum(df['none_search_type'])
        print(f'number of {num_rental_searches}')
        print(f'number of {num_sale_searches}')

        values = (day, num_searches, num_users,
                  num_rental_searches, num_sale_searches, num_none_searches)
        columns = "(day, num_searches, num_users, num_rental_searches, num_sale_searches,\
                                num_none_searches)"

        load_sql = LoadStatsRedshift.load_search_stats_sql.format(
            self.table,
            columns,
            values
        )

        # load data into redshift
        redshift_hook.run(load_sql)
