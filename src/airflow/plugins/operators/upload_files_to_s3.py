import os
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook


class UploadFilesToS3Operator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_conn_id='aws_credentials',
                 bucket_name='my-random-bucket-997',
                 path='',
                 *args, **kwargs):
        super(UploadFilesToS3Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.path = path

    def execute(self, context):
        s3_hook = S3Hook(self.aws_conn_id)

        for subdir, dirs, files in os.walk(self.path):
            for file in files:
                full_path = os.path.join(subdir, file)
                key = full_path[full_path.rindex('/', 0, full_path.rindex('/') - 1) + 1:]
                print(key)
                s3_hook.load_file(filename=full_path, bucket_name=self.bucket_name, key=key)
                self.log.info(f'Uploaded {full_path} file to s3://{self.bucket_name}/{key} bucket.')
