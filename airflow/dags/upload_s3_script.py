from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_callable(file_name, file_location, bucket_name):
    s3 = S3Hook(aws_conn_id= 'aws_default')
    s3.load_file(filename=file_location,bucket_name=bucket_name,key=file_name)