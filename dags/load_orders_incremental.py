import os
from datetime import timedelta

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from sqlalchemy import create_engine, text

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT_ID")
DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
LOOKBACK_DAYS = 7
COLS = ["order_id", "user_id", "status", "gender", "created_at",
        "shipped_at", "delivered_at", "returned_at", "num_of_item", "ingested_at"]

engine = create_engine(DB_URL)

# 1. Watermark: raw_orders dagi eng katta created_at, minus lookback
with engine.connect() as conn:
    max_created = conn.execute(text("SELECT MAX(created_at) FROM raw_orders")).scalar()
start = max_created - timedelta(days=LOOKBACK_DAYS)

# 2. BigQuery'dan faqat shu oraliqdagi qatorlarni olish
client = bigquery.Client(project=PROJECT)
query = """
    SELECT order_id, user_id, status, gender, created_at,
           shipped_at, delivered_at, returned_at, num_of_item
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    WHERE created_at >= @start
"""
job_config = bigquery.QueryJobConfig(query_parameters=[
    bigquery.ScalarQueryParameter("start", "TIMESTAMP", start)
])
df = client.query(query, job_config=job_config).to_dataframe()
print(f"BigQuery'dan olindi: {len(df)} qator (start={start})")

if df.empty:
    print("Yangi ma'lumot yo'q.")
    raise SystemExit(0)

df["ingested_at"] = pd.Timestamp.now(tz="UTC")

# 3. Vaqtinchalik jadvalga yozish, keyin upsert
df.to_sql("raw_orders_stage", engine, if_exists="replace", index=False)

update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLS if c != "order_id")
cols_sql = ", ".join(COLS)

with engine.begin() as conn:
    conn.execute(text(f"""
        INSERT INTO raw_orders ({cols_sql})
        SELECT {cols_sql} FROM raw_orders_stage
        ON CONFLICT (order_id) DO UPDATE SET {update_set}
    """))
    conn.execute(text("DROP TABLE raw_orders_stage"))

with engine.connect() as conn:
    total = conn.execute(text("SELECT COUNT(*) FROM raw_orders")).scalar()
print(f"raw_orders jami: {total} qator")