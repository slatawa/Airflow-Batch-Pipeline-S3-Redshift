from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
from _datetime import datetime
import boto3
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import sys
from operators.s3_user_clean_operator import UserCleanOperator
from operators.load_stats_redshift import LoadStatsRedshift
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

bucket_key_val = Variable.get('bucket_key')
source_key_val = Variable.get('source_key')
dest_key_val = Variable.get('dest_key')


def _check_s3_conn():
    conn = BaseHook.get_connection(conn_id='aws_conn')

    s3 = boto3.resource(service_name='s3',
                        region_name='us-east-1',
                        aws_access_key_id=conn.login,
                        aws_secret_access_key=conn.password)

    bucket = s3.Bucket(bucket_key_val)

    if bucket.creation_date:
        print("The bucket exists")
    else:
        print("The bucket does not exist")
    print('getting connection')


def _check_s3_conn_hook():
    s3hook = S3Hook(aws_conn_id='aws_conn')
    ret = s3hook.check_for_bucket(bucket_name=bucket_key_val)
    if ret:
        print("The bucket exists")
    else:
        print("The bucket does not exist")


default_args = {'owner': 'slatawa',
                'start_date': datetime.utcnow() - timedelta(days=24),
                'depends_on_past': False,
                'email_on_retry': False,
                'retry_delay': timedelta(minutes=1),
                'concurrency': 1,
                'max_active_runs': 1,
                'catchup ': True}

dag = DAG(dag_id='user-s3-to-redshift', default_args=default_args, \
          schedule_interval='@daily', catchup=True)

start_task = DummyOperator(task_id='start_task', dag=dag)

create_postgress_table = PostgresOperator(task_id='create_postgress_table',
                                          dag=dag,
                                          postgres_conn_id='redshift',
                                          sql='''
                                          create table if not exists public.search_stats (
                                          day date,
                                          num_users int,
                                          num_searches int,
                                          num_rental_searches int,
                                          num_sale_searches int,
                                          num_none_searches int)
                                          ''')

check_s3_conn = PythonOperator(task_id='check_s3_conn',
                               python_callable=_check_s3_conn_hook, dag=dag)

user_clean_data = UserCleanOperator(task_id='user_clean_data'
                                    , dag=dag
                                    , aws_conn_id='aws_conn'
                                    , aws_source_bucket=bucket_key_val
                                    , aws_source_key=source_key_val
                                    , aws_destination_bucket=bucket_key_val
                                    , aws_dest_key=dest_key_val)

load_table_data = LoadStatsRedshift(task_id='load_table_data'
                                    , dag=dag
                                    , postgres_conn_id='redshift'
                                    , aws_conn_id='aws_conn'
                                    , table='search_stats'
                                    , aws_destination_bucket=bucket_key_val
                                    , aws_dest_key=dest_key_val)

end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> create_postgress_table >> check_s3_conn >> user_clean_data >> load_table_data >> end_task
