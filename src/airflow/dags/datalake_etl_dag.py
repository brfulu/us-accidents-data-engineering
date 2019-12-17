import airflow
import configparser
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from operators import CreateS3BucketOperator, UploadFilesToS3Operator

# config = configparser.ConfigParser()
# config.read('../dl.cfg')

spark_script_bucket_name = 'fulu-spark-script'
raw_datalake_bucket_name = 'fulu-raw-datalake'
accidents_datalake_bucket_name = 'fulu-accidents-datalake'

default_args = {
    'owner': 'brfulu',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': 300,
    'email_on_retry': False
}

SPARK_TEST_STEPS = [
    {
        'Name': 'Setup Debugging',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    },
    {
        'Name': 'Setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', 's3://' + spark_script_bucket_name, '/home/hadoop/', '--recursive']
        }
    },
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/etl.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + accidents_datalake_bucket_name]
        }
    },
    {
        'Name': 'Setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', 's3://' + spark_script_bucket_name, '/home/hadoop/', '--recursive']
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Accidents-Datalake-ETL'
}

dag = DAG('accidents_datalake_etl_dag',
          default_args=default_args,
          description='Extract transform and load data to S3 datalake.',
          schedule_interval='@yearly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_code_bucket = CreateS3BucketOperator(
    task_id='Create_code_bucket',
    bucket_name=spark_script_bucket_name,
    dag=dag
)

upload_etl_script = UploadFilesToS3Operator(
    task_id='Upload_etl_script',
    bucket_name=spark_script_bucket_name,
    path='/opt/bitnami/script/',
    dag=dag
)

create_datalake_bucket = CreateS3BucketOperator(
    task_id='Create_datalake_bucket',
    bucket_name=accidents_datalake_bucket_name,
    dag=dag
)

create_cluster = EmrCreateJobFlowOperator(
    task_id='Create_EMR_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    dag=dag
)

add_steps = EmrAddStepsOperator(
    task_id='Add_jobflow_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

check_steps = EmrStepSensor(
    task_id='Watch_jobflow_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[2] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

delete_cluster = EmrTerminateJobFlowOperator(
    task_id='Delete_EMR_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_datalake_bucket >> create_cluster
start_operator >> create_code_bucket >> upload_etl_script >> create_cluster
create_cluster >> add_steps >> check_steps >> delete_cluster >> end_operator
