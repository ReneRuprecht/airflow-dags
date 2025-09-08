from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from scraper.tasks import db_tasks, s3_tasks, transform_tasks

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

S3URL = Variable.get("s3_url")
DOCKER_REGISTRY = Variable.get("docker_reg")


with DAG(
    "scrape_data_k8s",
    start_date=datetime(2025, 8, 22),
    catchup=False,
    default_args=default_args,
) as dag:

    markets = db_tasks.fetch_markets_from_postgres()

    scrape_products = KubernetesPodOperator.partial(
        task_id="scrape_data",
        name="scrape-data",
        namespace="airflow",
        image=f"{DOCKER_REGISTRY}/scraper:latest",
        on_finish_action="delete_pod",
        get_logs=True,
    ).expand(
        env_vars=markets,
    )

    files = s3_tasks.fetch_s3_file_list()
    products = transform_tasks.from_s3_to_products.expand(key=files)
    init_db = db_tasks.init_db()
    load = db_tasks.load_products_to_db.expand(products=products)

    scrape_products.set_downstream(files)
    load.set_upstream(init_db)
