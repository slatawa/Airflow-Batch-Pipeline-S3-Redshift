from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import boto3
import tempfile
from airflow.hooks.base import BaseHook
import io
from io import StringIO
import os
from helpers.utils import extract_valid_search_data, calc_avg_listings, calc_search_type


class UserCleanOperator(BaseOperator):
    def __init__(self, aws_conn_id,
                 aws_source_bucket, aws_source_key,
                 aws_destination_bucket, aws_dest_key, *args, **kwargs):
        super(UserCleanOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.aws_source_bucket = aws_source_bucket
        self.aws_source_key = aws_source_key
        self.aws_destination_bucket = aws_destination_bucket
        self.aws_dest_key = aws_dest_key

    def execute(self, context):
        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        source_file_obj = hook.get_key(bucket_name=self.aws_source_bucket, key=self.aws_source_key.format(**context))
        tf = tempfile.NamedTemporaryFile()
        tmp_file_name = tf.name
        source_file_obj.download_file(tmp_file_name)
        df = pd.read_csv(tmp_file_name, compression='zip', header=0, sep=',', quotechar='"')
        os.remove(tmp_file_name)

        # apply transformation
        df.drop_duplicates(inplace=True)
        df['valid_search_data'] = df['user_data'].apply(extract_valid_search_data)
        df['num_eligible_searches'] = df['valid_search_data'].apply(lambda x: len(x))
        # retain eligible searches
        df = df[df['num_eligible_searches'] > 0]
        df['avg_listings'] = df['valid_search_data'].apply(calc_avg_listings)
        temp = df['valid_search_data'].apply(calc_search_type)
        df['num_rental_search_type'] = temp['rental_search']
        df['num_sale_search_type'] = temp['sales_search']
        df['none_search_type'] = temp['none_type']

        # write file to s3
        conn = BaseHook.get_connection(conn_id=self.aws_conn_id)

        s3 = boto3.resource(service_name='s3',
                            region_name='us-east-1',
                            aws_access_key_id=conn.login,
                            aws_secret_access_key=conn.password)

        print('trying to write the object ')
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3.Object(self.aws_destination_bucket, self.aws_dest_key.format(**context)).put(Body=csv_buffer.getvalue())
        print('trying to write the object -2')







