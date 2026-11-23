import logging
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from sqlalchemy import create_engine, text

load_dotenv()

# ---- Sozlamalar (.env dan) ----
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
SOURCE = "bigquery-public-data.thelook_ecommerce"
DEFAULT_TABLES = ["orders", "users", "products", "distribution_centers"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("load_raw")


def extract(client: bigquery.Client, table: str) -> pd.DataFrame:
    """BigQuery jadvalini to'liq o'qiydi."""
    sql = f"SELECT * FROM `{SOURCE}.{table}`"
    df = client.query(sql).to_dataframe()
    log.info("%s: BigQuery'dan %s qator, %s ustun olindi", table, *df.shape)
    return df


def load_raw(df: pd.DataFrame, table: str, engine) -> None:
    """raw_<jadval> ga yozadi (har safar to'liq almashtiriladi)."""
    df = df.copy()
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")
    df.to_sql(f"raw_{table}", engine, if_exists="replace", index=False, chunksize=10_000)


def reconcile(df: pd.DataFrame, table: str, engine) -> None:
    """Manba va baza qatorlar sonini solishtiradi."""
    with engine.connect() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM raw_{table}")).scalar()
    if n != len(df):
        raise ValueError(f"{table}: manbada {len(df)} qator, bazada {n} qator, mos kelmadi")
    log.info("raw_%s: %s qator yozildi, soni mos ✅", table, n)


def main() -> None:
    tables = sys.argv[1:] or DEFAULT_TABLES
    client = bigquery.Client(project=GCP_PROJECT_ID)
    engine = create_engine(DB_URL)

    for table in tables:
        df = extract(client, table)
        load_raw(df, table, engine)
        reconcile(df, table, engine)


if __name__ == "__main__":
    main()