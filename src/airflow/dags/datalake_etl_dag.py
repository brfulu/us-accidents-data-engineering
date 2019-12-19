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

spark_script_bucket_name = 'fulu-spark-script'
raw_datalake_bucket_name = 'fulu-raw-datalake'
accidents_datalake_bucket_name = 'fulu-accidents-datalake'

default_args = {
    'owner': 'brfulu',
    'start_date': datetime(2019, 10, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

SPARK_ETL_STEPS = [
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
        'Name': 'Airports - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/airport_etl.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + accidents_datalake_bucket_name]
        }
    },
    {
        'Name': 'Cities - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/city_etl.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + accidents_datalake_bucket_name]
        }
    },
    {
        'Name': 'Accidents - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/accident_etl.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + accidents_datalake_bucket_name]
        }
    },
    {
        'Name': 'Check data quality',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/check_data_quality.py',
                     's3a://' + accidents_datalake_bucket_name]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Accidents-Datalake-ETL'
}

dag = DAG('accidents_datalake_etl_dag',
          default_args=default_args,
          description='Extract transform and load data to S3 datalake.',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_code_bucket = CreateS3BucketOperator(
    task_id='Create_code_bucket',
    bucket_name=spark_script_bucket_name,
    dag=dag
)

upload_etl_code = UploadFilesToS3Operator(
    task_id='Upload_etl_code',
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

add_jobflow_steps = EmrAddStepsOperator(
    task_id='Add_jobflow_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=SPARK_ETL_STEPS,
    dag=dag
)

check_city_processing = EmrStepSensor(
    task_id='Watch_city_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[3] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

check_airport_processing = EmrStepSensor(
    task_id='Watch_airport_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[2] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

check_accident_processing = EmrStepSensor(
    task_id='Watch_accident_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[4] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

check_data_quality_check = EmrStepSensor(
    task_id='Watch_data_quality_check_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[5] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

delete_cluster = EmrTerminateJobFlowOperator(
    task_id='Delete_EMR_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_datalake_bucket >> create_cluster
start_operator >> create_code_bucket >> upload_etl_code >> create_cluster
create_cluster >> add_jobflow_steps
add_jobflow_steps >> check_city_processing >> check_data_quality_check
add_jobflow_steps >> check_airport_processing >> check_data_quality_check
add_jobflow_steps >> check_accident_processing >> check_data_quality_check
check_data_quality_check >> delete_cluster >> end_operator
