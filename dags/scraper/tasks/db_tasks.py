import datetime
import os

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

S3URL = Variable.get("s3_url")


@task
def init_db(conn_id="products"):
    DAG_DIR = os.path.dirname(os.path.realpath(__file__))

    sql_file = os.path.join(DAG_DIR, "../sql/create_tables.sql")
    with open(sql_file, "r") as f:
        sql = f.read()

    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run(sql)


@task
def load_products_to_db(products: list[dict]):

    today = datetime.date.today()
    hook = PostgresHook(postgres_conn_id="products")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            product_records = [
                (
                    p["id"],
                    p["name"],
                    p.get("brand"),
                    p["unit"],
                    p.get("base_price_value"),
                    p.get("base_price_unit"),
                    p.get("market"),
                    p.get("valid_from"),
                    p.get("valid_to"),
                    p.get("info"),
                    today,
                )
                for p in products
            ]

            cur.executemany(
                """
                INSERT INTO products 
                    (id, name, brand, unit, base_price_value, base_price_unit, market, valid_from, valid_to, info, created_at)
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
                product_records,
            )

            price_records = []
            for p in products:
                for price in p.get("prices", []):
                    price_records.append(
                        (
                            p["id"],
                            price["price_type"],
                            price["amount"],
                            price.get("condition"),
                        )
                    )

            if price_records:
                cur.executemany(
                    """
                    INSERT INTO prices (product_id, price_type, amount, condition)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (product_id, price_type) DO UPDATE
                    SET amount = EXCLUDED.amount,
                        condition = EXCLUDED.condition;
                    """,
                    price_records,
                )

            discount_records = []
            for p in products:
                for disc in p.get("discount_percents", []):
                    discount_records.append(
                        (
                            p["id"],
                            disc["discount_type"],
                            disc["amount"],
                            disc.get("condition"),
                        )
                    )

            if discount_records:
                cur.executemany(
                    """
                    INSERT INTO discounts (product_id, discount_type, amount, condition)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (product_id, discount_type) DO UPDATE
                    SET amount = EXCLUDED.amount,
                        condition = EXCLUDED.condition;
                    """,
                    discount_records,
                )

        conn.commit()


@task
def fetch_markets_from_postgres():
    hook = PostgresHook(postgres_conn_id="markets")
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
