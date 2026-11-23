import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_DIR = os.getenv("THELOOK_DIR", "/opt/airflow/dags")
load_dotenv(os.path.join(PROJECT_DIR, ".env"))


def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def run_statements(statements):
    engine = get_engine()
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


STAGING_SQL = [
    "DROP TABLE IF EXISTS stg_orders",
    """CREATE TABLE stg_orders AS
    SELECT order_id, user_id, TRIM(status) AS status, LOWER(TRIM(gender)) AS gender,
           created_at, shipped_at, delivered_at, returned_at, num_of_item,
           (shipped_at IS NOT NULL AND shipped_at < created_at) AS is_shipped_before_created,
           (created_at > NOW()) AS is_future_created_at, ingested_at
    FROM raw_orders
    WHERE order_id IS NOT NULL AND user_id IS NOT NULL AND created_at IS NOT NULL""",

    "DROP TABLE IF EXISTS stg_users",
    """CREATE TABLE stg_users AS
    SELECT id AS user_id, LOWER(TRIM(email)) AS email, TRIM(first_name) AS first_name,
           TRIM(last_name) AS last_name, age, LOWER(TRIM(gender)) AS gender,
           TRIM(country) AS country, TRIM(city) AS city, created_at, ingested_at
    FROM raw_users
    WHERE id IS NOT NULL AND email IS NOT NULL AND TRIM(email) <> ''""",

    "DROP TABLE IF EXISTS stg_products",
    """CREATE TABLE stg_products AS
    SELECT id AS product_id, TRIM(category) AS category, TRIM(name) AS product_name,
           TRIM(brand) AS brand, retail_price, cost, department, ingested_at
    FROM raw_products
    WHERE id IS NOT NULL AND retail_price >= 0""",

    "DROP TABLE IF EXISTS stg_order_items",
    """CREATE TABLE stg_order_items AS
    SELECT id AS order_item_id, order_id, user_id, product_id, TRIM(status) AS status,
           created_at, shipped_at, delivered_at, returned_at, sale_price,
           (shipped_at IS NOT NULL AND shipped_at < created_at) AS is_shipped_before_created,
           (created_at > NOW()) AS is_future_created_at, ingested_at
    FROM raw_order_items
    WHERE id IS NOT NULL AND order_id IS NOT NULL AND product_id IS NOT NULL AND sale_price > 0""",
]

DW_SQL = [
    "DROP TABLE IF EXISTS dim_users",
    """CREATE TABLE dim_users AS
    SELECT user_id, first_name, last_name, email, age, gender,
           COALESCE(city, 'Unknown') AS city, country, created_at AS user_created_at
    FROM stg_users""",
    "ALTER TABLE dim_users ADD PRIMARY KEY (user_id)",

    "DROP TABLE IF EXISTS dim_products",
    """CREATE TABLE dim_products AS
    SELECT product_id, product_name, brand, category, department, retail_price, cost
    FROM stg_products""",
    "ALTER TABLE dim_products ADD PRIMARY KEY (product_id)",

    "DROP TABLE IF EXISTS fact_order_items",
    """CREATE TABLE fact_order_items AS
    SELECT order_item_id, order_id, user_id, product_id, status,
           created_at AS order_item_created_at, shipped_at, delivered_at, returned_at,
           sale_price, is_shipped_before_created, is_future_created_at
    FROM stg_order_items""",
    "ALTER TABLE fact_order_items ADD PRIMARY KEY (order_item_id)",
]

MART_SQL = [
    "DROP TABLE IF EXISTS mart_daily_revenue",
    """CREATE TABLE mart_daily_revenue AS
    SELECT DATE(order_item_created_at) AS order_date, COUNT(*) AS items_sold,
           ROUND(SUM(sale_price)::numeric, 2) AS revenue
    FROM fact_order_items
    WHERE status NOT IN ('Cancelled', 'Returned') AND NOT is_future_created_at
    GROUP BY DATE(order_item_created_at)
    ORDER BY order_date""",

    "DROP TABLE IF EXISTS mart_top_categories",
    """CREATE TABLE mart_top_categories AS
    SELECT p.category, COUNT(*) AS items_sold, ROUND(SUM(f.sale_price)::numeric, 2) AS revenue
    FROM fact_order_items f
    JOIN dim_products p ON f.product_id = p.product_id
    WHERE f.status NOT IN ('Cancelled', 'Returned') AND NOT f.is_future_created_at
    GROUP BY p.category
    ORDER BY revenue DESC""",
]


def build_staging():
    run_statements(STAGING_SQL)


def build_dw():
    run_statements(DW_SQL)


def build_marts():
    run_statements(MART_SQL)


def data_quality_checks():
    engine = get_engine()
    with engine.connect() as conn:
        raw_orders = conn.execute(text("SELECT COUNT(*) FROM raw_orders")).scalar()
        raw_items_dup = conn.execute(text(
            "SELECT COUNT(*) - COUNT(DISTINCT id) FROM raw_order_items")).scalar()
        stg_items = conn.execute(text("SELECT COUNT(*) FROM stg_order_items")).scalar()
        fact_items = conn.execute(text("SELECT COUNT(*) FROM fact_order_items")).scalar()
        shipped_flags = conn.execute(text(
            "SELECT COUNT(*) FILTER (WHERE is_shipped_before_created) FROM fact_order_items")).scalar()

    errors = []
    if raw_orders == 0:
        errors.append("raw_orders bo'sh")
    if raw_items_dup > 0:
        errors.append(f"raw_order_items da {raw_items_dup} ta takror id bor")
    if fact_items != stg_items:
        errors.append(f"fact_order_items ({fact_items}) stg_order_items ({stg_items}) bilan teng emas")

    # Ogohlantirish: qatorni to'xtatmaydi, faqat log'ga yozadi
    if shipped_flags > 40000:
        print(f"OGOHLANTIRISH: shipped_before_created flag {shipped_flags} (odatda ~35691)")

    if errors:
        raise ValueError("; ".join(errors))
    print(f"DQ o'tdi: raw_orders={raw_orders}, fact_order_items={fact_items}")


default_args = {
    "owner": "qosim",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="thelook_pipeline",
    default_args=default_args,
    description="TheLook: BigQuery -> raw -> stg -> dim/fact -> marts",
    start_date=datetime(2026, 11, 20),
    schedule="@daily",
    catchup=False,
    tags=["thelook", "portfolio"],
) as dag:

    load_orders = BashOperator(
        task_id="load_orders_incremental",
        bash_command=f'python "{os.path.join(PROJECT_DIR, "load_orders_incremental.py")}"',
    )

    load_order_items = BashOperator(
        task_id="load_order_items_incremental",
        bash_command=f'python "{os.path.join(PROJECT_DIR, "load_order_items_incremental.py")}"',
    )

    stg = PythonOperator(task_id="build_staging", python_callable=build_staging)
    dw = PythonOperator(task_id="build_dw", python_callable=build_dw)
    marts = PythonOperator(task_id="build_marts", python_callable=build_marts)
    dq = PythonOperator(task_id="data_quality_checks", python_callable=data_quality_checks)

    load_orders >> load_order_items >> stg >> dw >> dq >> marts