

SELECT COUNT(*) AS raw_orders_cnt FROM raw_orders;
SELECT COUNT(*) AS raw_users_cnt FROM raw_users;
SELECT COUNT(*) AS raw_products_cnt FROM raw_products;
SELECT COUNT(*) AS raw_order_items_cnt FROM raw_order_items;
SELECT COUNT(*) AS raw_inventory_items_cnt FROM raw_inventory_items;


SELECT 
    status,
    COUNT(*) AS order_count
FROM raw_orders
GROUP BY status
ORDER BY order_count DESC;


SELECT 
    DATE(created_at) AS order_date,
    ROUND(SUM(sale_price)::numeric, 2) AS daily_revenue
FROM raw_order_items
WHERE status NOT IN ('Cancelled', 'Returned')
  AND created_at >= (
      SELECT MAX(created_at) - INTERVAL '7 days'
      FROM raw_order_items
  )
GROUP BY DATE(created_at)
ORDER BY order_date DESC;

SELECT 
    p.category,
    ROUND(SUM(oi.sale_price)::numeric, 2) AS total_revenue,
    COUNT(*) AS items_sold
FROM raw_order_items oi
JOIN raw_products p
    ON oi.product_id = p.id
WHERE oi.status NOT IN ('Cancelled', 'Returned')
GROUP BY p.category
ORDER BY total_revenue DESC
LIMIT 5;

SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_diff
FROM raw_orders;

SELECT 
    (SELECT COUNT(*)
     FROM raw_orders
     WHERE returned_at IS NOT NULL
       AND created_at IS NOT NULL
       AND returned_at < created_at) AS invalid_return_dates,
    (SELECT COUNT(*)
     FROM raw_users
     WHERE email IS NULL
        OR TRIM(email) = '') AS empty_emails;



CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT
    order_id,
    user_id,
    TRIM(status) AS status,
    LOWER(TRIM(gender)) AS gender,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    num_of_item,
    ingested_at
FROM raw_orders
WHERE order_id IS NOT NULL
  AND user_id IS NOT NULL
  AND created_at IS NOT NULL
  AND (returned_at IS NULL OR returned_at >= created_at);

CREATE TABLE IF NOT EXISTS stg_users AS
SELECT
    id AS user_id,
    LOWER(TRIM(email)) AS email,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    age,
    LOWER(TRIM(gender)) AS gender,
    TRIM(country) AS country,
    TRIM(city) AS city,
    created_at,
    ingested_at
FROM raw_users
WHERE id IS NOT NULL
  AND email IS NOT NULL
  AND TRIM(email) <> '';

CREATE TABLE IF NOT EXISTS stg_products AS
SELECT
    id AS product_id,
    TRIM(category) AS category,
    TRIM(name) AS product_name,
    TRIM(brand) AS brand,
    retail_price,
    cost,
    department,
    ingested_at
FROM raw_products
WHERE id IS NOT NULL
  AND retail_price >= 0;

CREATE TABLE IF NOT EXISTS stg_order_items AS
SELECT
    id AS order_item_id,
    order_id,
    user_id,
    product_id,
    TRIM(status) AS status,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    sale_price,
    ingested_at
FROM raw_order_items
WHERE id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND sale_price >= 0;


SELECT 'stg_orders' AS tbl, COUNT(*) AS cnt FROM stg_orders
UNION ALL
SELECT 'stg_users', COUNT(*) FROM stg_users
UNION ALL
SELECT 'stg_products', COUNT(*) FROM stg_products
UNION ALL
SELECT 'stg_order_items', COUNT(*) FROM stg_order_items;

SELECT
    -- 1. NULL lar
    COUNT(*) FILTER (WHERE sale_price IS NULL) AS null_sale_price,
    COUNT(*) FILTER (WHERE created_at IS NULL) AS null_created_at,
    -- 2. Manfiy / 0 narxlar
    COUNT(*) FILTER (WHERE sale_price <= 0) AS non_positive_price,
    -- 3. Sana ketma-ketligi buzilishi
    COUNT(*) FILTER (WHERE shipped_at IS NOT NULL AND shipped_at < created_at) AS shipped_before_created,
    COUNT(*) FILTER (WHERE delivered_at IS NOT NULL AND shipped_at IS NOT NULL AND delivered_at < shipped_at) AS delivered_before_shipped,
    -- 4. Kelajakdagi sana
    COUNT(*) FILTER (WHERE created_at > NOW()) AS future_created_at,
    -- 5. Orphan (raw_orders da yo'q order_id)
    COUNT(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM raw_orders o WHERE o.order_id = raw_order_items.order_id
    )) AS orphan_orders
FROM raw_order_items;


DROP TABLE IF EXISTS stg_order_items;

CREATE TABLE stg_order_items AS
SELECT
    id AS order_item_id,
    order_id,
    user_id,
    product_id,
    TRIM(status) AS status,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    sale_price,
    (shipped_at IS NOT NULL AND shipped_at < created_at) AS is_shipped_before_created,
    (created_at > NOW()) AS is_future_created_at,
    ingested_at
FROM raw_order_items
WHERE id IS NOT NULL
  AND order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND sale_price > 0;

SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE is_shipped_before_created) AS shipped_before_created_cnt,
    COUNT(*) FILTER (WHERE is_future_created_at) AS future_created_cnt
FROM stg_order_items;



DROP TABLE IF EXISTS stg_orders;
CREATE TABLE stg_orders AS
SELECT
    order_id,
    user_id,
    TRIM(status) AS status,
    LOWER(TRIM(gender)) AS gender,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    num_of_item,
    (shipped_at IS NOT NULL AND shipped_at < created_at) AS is_shipped_before_created,
    (created_at > NOW()) AS is_future_created_at,
    ingested_at
FROM raw_orders
WHERE order_id IS NOT NULL
  AND user_id IS NOT NULL
  AND created_at IS NOT NULL;

DROP TABLE IF EXISTS stg_users;
CREATE TABLE stg_users AS
SELECT
    id AS user_id,
    LOWER(TRIM(email)) AS email,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    age,
    LOWER(TRIM(gender)) AS gender,
    TRIM(country) AS country,
    TRIM(city) AS city,
    created_at,
    ingested_at
FROM raw_users
WHERE id IS NOT NULL
  AND email IS NOT NULL
  AND TRIM(email) <> '';

DROP TABLE IF EXISTS stg_products;
CREATE TABLE stg_products AS
SELECT
    id AS product_id,
    TRIM(category) AS category,
    TRIM(name) AS product_name,
    TRIM(brand) AS brand,
    retail_price,
    cost,
    department,
    ingested_at
FROM raw_products
WHERE id IS NOT NULL
  AND retail_price >= 0;


SELECT 'stg_orders' AS tbl, COUNT(*) AS cnt FROM stg_orders
UNION ALL
SELECT 'stg_users', COUNT(*) FROM stg_users
UNION ALL
SELECT 'stg_products', COUNT(*) FROM stg_products
UNION ALL
SELECT 'stg_order_items', COUNT(*) FROM stg_order_items;

SELECT COUNT(*) FROM stg_orders

SELECT COUNT(*) FROM stg_users;
SELECT COUNT(*) FROM raw_users;

SELECT COUNT(*) FROM stg_products;
SELECT COUNT(*) FROM raw_products;

SELECT * FROM stg_users LIMIT 5;



SELECT column_name FROM information_schema.columns
WHERE table_name = 'stg_users';

DROP TABLE IF EXISTS dim_users;
CREATE TABLE dim_users AS
SELECT
    user_id AS user_id,
    first_name,
    last_name,
    email,
    age,
    gender,
    COALESCE(city, 'Unknown') AS city,
    c,
    country,
    traffic_source,
    created_at AS user_created_at
FROM stg_users;

ALTER TABLE dim_users ADD PRIMARY KEY (user_id);


DROP TABLE IF EXISTS dim_products;
CREATE TABLE dim_products AS
SELECT
    id AS product_id,
    name AS product_name,
    brand,
    category,
    department,
    retail_price,
    cost
FROM stg_products;

ALTER TABLE dim_products ADD PRIMARY KEY (product_id);

DROP TABLE IF EXISTS fact_order_items;
CREATE TABLE fact_order_items AS
SELECT
    id AS order_item_id,
    order_id,
    user_id,
    product_id,
    status,
    created_at AS order_item_created_at,
    sale_price,
    is_shipped_before_created,
    is_future_created_at
FROM stg_order_items;

ALTER TABLE fact_order_items ADD PRIMARY KEY (order_item_id);


SELECT COUNT(*) FROM dim_users;     
SELECT COUNT(*) FROM dim_products;   
SELECT COUNT(*) FROM fact_order_items; 
SELECT COUNT(*) FROM dim_users WHERE city = 'Unknown'; -- city null'lar soni