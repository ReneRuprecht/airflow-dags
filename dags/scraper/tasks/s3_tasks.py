from datetime import datetime

import boto3
from airflow.decorators import task
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

    if "Contents" not in response:
        return []

    today = datetime.today()
    today_filter = (
        f"year={today.year}/month={today.strftime('%m')}/day={today.strftime('%d')}/"
    )

    objects = [
        obj
        for obj in response["Contents"]
        if obj["Key"].endswith(".json") and today_filter in obj["Key"]
    ]

    if not objects:
        return []

    latest_files = {}
    for obj in objects:
        marketid = obj["Key"].split("/")[2]
        if (
            marketid not in latest_files
            or obj["LastModified"] > latest_files[marketid]["LastModified"]
        ):
            latest_files[marketid] = obj

    return [obj["Key"] for obj in latest_files.values()]
