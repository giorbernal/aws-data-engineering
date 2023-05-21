import os
import boto3


def get_session():
    aws_local_mode = os.environ.get('AWS_LOCAL_MODE', 'False') == 'True'
    # Initiate session
    if aws_local_mode:
        # Set AWS credentials
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_region = os.environ.get('AWS_REGION')
        # create session with AWS credentials
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
    else:
        # create session without AWS credentials
        session = boto3.Session()

    return session