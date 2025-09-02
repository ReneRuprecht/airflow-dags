import datetime
import os

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scraper.models.product import Product

S3URL = Variable.get("s3_url")


def insert_product(product: Product):
    today = datetime.date.today()

    hook = PostgresHook(postgres_conn_id="products")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO products (id, name, brand, unit, base_price_value, base_price_unit, market, valid_from, valid_to, info, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET name = EXCLUDED.name,
            brand = EXCLUDED.brand,
            unit = EXCLUDED.unit,
            base_price_value = EXCLUDED.base_price_value,
            base_price_unit = EXCLUDED.base_price_unit,
            market = EXCLUDED.market,
            valid_from = EXCLUDED.valid_from,
            valid_to = EXCLUDED.valid_to,
            info = EXCLUDED.info,
            created_at = EXCLUDED.created_at;
    """,
        (
            product.id,
            product.name,
            product.brand,
            product.unit,
            product.base_price_value,
            product.base_price_unit,
            product.market,
            product.valid_from,
            product.valid_to,
            product.info,
            today,
        ),
    )

    for price in product.prices:
        cur.execute(
            """
            INSERT INTO prices (product_id, price_type, amount, condition)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id, price_type) DO UPDATE
            SET amount = EXCLUDED.amount,
                condition = EXCLUDED.condition;
        """,
            (product.id, price.price_type, price.amount, price.condition),
        )

    for disc in product.discount_percents:
        cur.execute(
            """
            INSERT INTO discounts (product_id, discount_type, amount, condition)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id, discount_type) DO UPDATE
            SET amount = EXCLUDED.amount,
                condition = EXCLUDED.condition;
        """,
            (product.id, disc.discount_type, disc.amount, disc.condition),
        )

    conn.commit()
    cur.close()
    conn.close()


@task
def init_db(conn_id="my_postgres"):
    DAG_DIR = os.path.dirname(os.path.realpath(__file__))

    sql_file = os.path.join(DAG_DIR, "../sql/create_tables.sql")
    with open(sql_file, "r") as f:
        sql = f.read()

    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run(sql)


@task
def load_products_to_db(products):
    for product in products:
        insert_product(product)


@task
def fetch_from_postgres():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    records = hook.get_records(
        "SELECT store_id, market, plz, city, street, url FROM markets;"
    )
    out = []

    for r in records:
        out.append(
            {
                "MARKET_ID": r[0],
                "MARKET": r[1],
                "MARKET_PLZ": r[2],
                "MARKET_CITY": r[3],
                "MARKET_STREET": r[4],
                "MARKET_URL": r[5],
                "S3URL": S3URL,
            }
        )
    return out
