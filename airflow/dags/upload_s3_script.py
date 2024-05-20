import boto3

def upload_callable(file_name, file_location, access_key, secret_key, bucket_name):
    print(access_key, secret_key, file_name)
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3 = session.client('s3')
    s3.upload_file(file_location,bucket_name,file_name)