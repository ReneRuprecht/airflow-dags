from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.models import Variable

from datetime import datetime, timedelta
from scraper.tasks import db_tasks, s3_tasks, transform_tasks

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

S3URL = Variable.get("s3_url")
DOCKER_REGISTRY = Variable.get("docker_reg")


with DAG(
    "scrape_data_k8s",
    start_date=datetime(2025, 8, 22),
    catchup=False,
    default_args=default_args,
) as dag:

    data = db_tasks.fetch_from_postgres()

    run_container = KubernetesPodOperator.partial(
        task_id="scrape_data",
        name="scrape-data",
        namespace="airflow",
        image=f"{DOCKER_REGISTRY}/scraper:latest",
        is_delete_operator_pod=True,
        get_logs=True,
    ).expand(
        env_vars=data,
    )

    files = s3_tasks.fetch_s3_file_list()
    products = transform_tasks.process_single_file.expand(key=files)
    db_tasks.load_products_to_db.expand(products=products)

    run_container.set_downstream(files)
