from airflow.decorators import task
import boto3
from datetime import datetime
from airflow.models import Variable

@task
def fetch_s3_file_list():
    bucket = "scraper-data"
    minio_endpoint = Variable.get("s3_url")
    access_key = Variable.get("s3_access")
    secret_key = Variable.get("s3_secret")

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    response = s3.list_objects_v2(Bucket=bucket, Prefix="raw/")
    keys = [obj["Key"] for obj in response.get("Contents", [])]

    today = datetime.today()
    today_filter = f"year={today.year}/month={today.strftime('%m')}/day={today.strftime('%d')}/"
    json_files = [k for k in keys if k.endswith(".json") and today_filter in k]

    return json_files
