import json

import boto3
from airflow.decorators import task
from airflow.models import Variable
from scraper.models.product import Product


@task
def from_s3_to_products(key: str):
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
    print(key)

    obj = s3.get_object(Bucket=bucket, Key=key)
    data = json.load(obj["Body"])

    s3_products = data["products"]

    products = [Product(**p).model_dump(exclude_unset=True) for p in s3_products]

    return products
