from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook


class CheckS3FileCount(BaseOperator):
    ui_color = '#B2CCFF'

    @apply_defaults
    def __init__(self,
                 aws_conn_id='aws_credentials',
                 bucket_name='my-random-bucket-997',
                 expected_count=0,
                 *args, **kwargs):
        super(CheckS3FileCount, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.expected_count = expected_count

    def execute(self, context):
        s3_hook = S3Hook(self.aws_conn_id)
        keys = s3_hook.list_keys(self.bucket_name)

        if len(keys) != self.expected_count:
            raise AssertionError(f'S3 file count doesnt match: {len(keys)} != {self.expected_count}')
