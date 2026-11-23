# TheLook eCommerce Data Pipeline

An end-to-end data engineering portfolio project. It loads the TheLook eCommerce dataset from BigQuery into PostgreSQL, builds a layered warehouse (raw → staging → star schema → marts), runs the process daily with Apache Airflow, and visualizes the results in Power BI.

## Architecture

```
BigQuery (bigquery-public-data.thelook_ecommerce)
        │  incremental extract (Python)
        ▼
PostgreSQL: raw_*        ← unchanged copy of the source
        │  cleaning + flags (SQL)
        ▼
PostgreSQL: stg_*        ← typed, trimmed, flagged
        │  star schema (SQL)
        ▼
dim_users, dim_products, fact_order_items
        │  aggregation (SQL)
        ▼
mart_daily_revenue, mart_top_categories
        │
        ▼
Power BI dashboard (DAX measures)
```

ELT approach: raw data is never modified. All transformations happen inside PostgreSQL, so every step can be re-run and checked.

## Tech stack

- Python 3.12: `google-cloud-bigquery`, `pandas`, `sqlalchemy`, `psycopg2`, `python-dotenv`
- Google BigQuery (public dataset)
- PostgreSQL 16 in Docker
- Apache Airflow 2.8 in Docker
- Power BI Desktop (DAX)

## Repository structure

```
thelook-pipeline/
├── dags/
│   ├── thelook_pipeline.py              # Airflow DAG: full daily pipeline
│   ├── load_orders_incremental.py       # raw_orders incremental load
│   ├── load_order_items_incremental.py  # raw_order_items incremental load
│   ├── load_raw.py                      # initial full load of raw_* tables
│   └── bq_test.py                       # BigQuery connection test
├── sql/
│   └── all_sql.sql                      # profiling, staging, star schema, marts
├── dashboard/
│   ├── screenshots/                     # dashboard screenshots
│   └── thelook_dashboard.pbix           # Power BI file
├── docker-compose.yaml
├── requirements.txt
├── .env.example
└── README.md
```

## Data model

| Table | Type | Description |
|---|---|---|
| `raw_orders`, `raw_order_items`, `raw_users`, `raw_products` | Raw | Copy of the BigQuery source |
| `stg_orders`, `stg_order_items`, `stg_users`, `stg_products` | Staging | Cleaned, trimmed, with quality flags |
| `dim_users` | Dimension | One row per customer |
| `dim_products` | Dimension | One row per product |
| `fact_order_items` | Fact | One row per item in an order |
| `mart_daily_revenue` | Mart | Daily revenue, excluding cancelled and returned items |
| `mart_top_categories` | Mart | Revenue by product category |

## Data quality

Checks run in the pipeline after the warehouse is built. A failed check stops the DAG before the marts are rebuilt.

- Duplicate `id` in `raw_order_items` → **error**
- Row count of `fact_order_items` differs from `stg_order_items` → **error**
- `raw_orders` is empty → **error**
- Too many `shipped_before_created` flags → **warning** only

Rows with date issues are not deleted. They are flagged:
- `is_shipped_before_created`: 35,691 rows (shipping date earlier than order date)
- `is_future_created_at`: 1,417 rows (order date in the future)

These come from the source data and are kept visible in the dashboard.

## Incremental loading

- Watermark: `created_at`, with a 7-day lookback window. Orders can change status after creation (`shipped_at`, `delivered_at`, `returned_at`), so recent rows are reloaded each run.
- Upsert: `INSERT ... ON CONFLICT (id) DO UPDATE` keeps the table free of duplicates.
- `raw_users` and `raw_products` change rarely, so they are loaded in full.

**Known limitation:** orders older than 7 days that change status later are not picked up by the daily run. A periodic full reload of `orders` would fix this.

## Setup

### 1. Prerequisites

- Python 3.12
- Docker Desktop
- A Google Cloud project with BigQuery enabled
- Power BI Desktop (optional, for the dashboard)

### 2. Google Cloud authentication

```bash
gcloud auth application-default login
```

Or use a service account JSON key. Keep the key file out of the repository (it is listed in `.gitignore`).

### 3. Environment variables

Copy `.env.example` to `.env` and fill in your values:

```
DB_USER=root
DB_PASSWORD=your_password
DB_HOST=127.0.0.1
DB_PORT=5434
DB_NAME=thelook
GCP_PROJECT_ID=your_project_id
AIRFLOW_DB_PASSWORD=your_airflow_password
AIRFLOW_SECRET_KEY=your_secret_key
```

### 4. Python dependencies

```bash
pip install -r requirements.txt
```

### 5. PostgreSQL

Start a PostgreSQL container on port 5434 and create the `thelook` database:

```bash
docker run -d --name pg-learn -p 5434:5432 -e POSTGRES_PASSWORD=your_password postgres:16
docker exec -it pg-learn psql -U postgres -c "CREATE DATABASE thelook;"
```

### 6. Initial load

```bash
python dags/load_raw.py
```

Then run the SQL in `sql/all_sql.sql` in order to build the staging, star schema, and mart tables.

### 7. Airflow

```bash
docker compose up -d
```

Open http://localhost:8080, unpause `thelook_pipeline`, and trigger it. The DAG runs daily.

## Results

- 181,885 order items, 125,481 orders, 100,000 customers, 29,120 products
- Top category by revenue: Outerwear & Coats (938,280.53)

## Dashboard

Three pages in Power BI:
1. **Overview:** KPIs, daily revenue, order status breakdown
2. **Products:** top categories, category/brand matrix, top 10 products
3. **Customers and data quality:** customers by country and age group, date quality flags

Screenshots are in `dashboard/screenshots/`.

## Author

Topilov Qosimjon · [GitHub](https://github.com/qosim006)