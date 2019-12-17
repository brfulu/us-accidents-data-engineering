from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import CreateS3BucketOperator, UploadFilesToS3Operator

# config = configparser.ConfigParser()
# config.read('../dl.cfg')

raw_datalake_bucket_name = 'fulu-raw-datalake'

default_args = {
    'owner': 'brfulu',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

dag = DAG('raw_datalake_dag',
          default_args=default_args,
          description='Load data into raw S3 datalake.',
          schedule_interval='@hourly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_raw_datalake = CreateS3BucketOperator(
    task_id='Create_raw_datalake',
    bucket_name=raw_datalake_bucket_name,
    dag=dag
)

upload_airport_data = UploadFilesToS3Operator(
    task_id='Upload_airport_data',
    bucket_name=raw_datalake_bucket_name,
    path='/opt/bitnami/dataset/airport_data/',
    dag=dag
)

upload_city_data = UploadFilesToS3Operator(
    task_id='Upload_city_data',
    bucket_name=raw_datalake_bucket_name,
    path='/opt/bitnami/dataset/city_data/',
    dag=dag
)

upload_accident_data = UploadFilesToS3Operator(
    task_id='Upload_accident_data',
    bucket_name=raw_datalake_bucket_name,
    path='/opt/bitnami/dataset/accident_data/',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_raw_datalake

create_raw_datalake >> upload_accident_data
create_raw_datalake >> upload_airport_data
create_raw_datalake >> upload_city_data

upload_accident_data >> end_operator
upload_airport_data >> end_operator
upload_city_data >> end_operator
