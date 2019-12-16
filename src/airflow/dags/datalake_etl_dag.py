import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

raw_datalake_bucket_name = 'fulu-raw-datalake-123'
accidents_datalake_bucket_name = 'fulu-accidents-datalake'

default_args = {
    'owner': 'brfulu',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 1,
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
            'Args': ['aws', 's3', 'cp', 's3://my-bucket', '/home/hadoop/', '--recursive']
        }
    },
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/etl.py', 'input_data', 'output_data']
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Accidents-Datalake-ETL'
}

dag = DAG('accidents_datalake_etl_dag',
          default_args=default_args,
          description='Extract transform and load data to S3 datalake.',
          schedule_interval='@hourly',
          catchup=False
          )

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    region_name='eu-west-1',
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

cluster_creator >> step_adder >> step_checker >> cluster_remover
