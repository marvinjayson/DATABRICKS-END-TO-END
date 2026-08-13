# Databricks notebook source
# DBTITLE 1,Bronze → Silver
# MAGIC %md
# MAGIC # Bronze → Silver
# MAGIC Transforms raw `sales.bronze.*` tables into clean, typed, and enriched `sales.silver.*` Delta tables.
# MAGIC Run all cells top-to-bottom.

# COMMAND ----------

# DBTITLE 1,silver.customers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.customers AS
# MAGIC SELECT DISTINCT
# MAGIC     customer_id,
# MAGIC     customer_unique_id,
# MAGIC     CAST(customer_zip_code_prefix AS INT)                          AS customer_zip_code_prefix,
# MAGIC     LOWER(TRIM(REGEXP_REPLACE(customer_city,  '\\s+', ' ')))      AS customer_city,
# MAGIC     UPPER(TRIM(REGEXP_REPLACE(customer_state, '\\s+', ' ')))      AS customer_state
# MAGIC FROM sales.bronze.customers
# MAGIC WHERE customer_id IS NOT NULL

# COMMAND ----------

# DBTITLE 1,silver.sellers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.sellers AS
# MAGIC WITH deduped AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_id) AS rn
# MAGIC     FROM sales.bronze.sellers
# MAGIC     WHERE seller_id IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     seller_id,
# MAGIC     CAST(seller_zip_code_prefix AS INT)                            AS seller_zip_code_prefix,
# MAGIC     LOWER(TRIM(REGEXP_REPLACE(seller_city,  '\\s+', ' ')))        AS seller_city,
# MAGIC     UPPER(TRIM(REGEXP_REPLACE(seller_state, '\\s+', ' ')))        AS seller_state
# MAGIC FROM deduped
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,silver.products
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.products AS
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         product_id,
# MAGIC         COALESCE(product_category_name, 'unknown_category')        AS product_category_name,
# MAGIC         CAST(product_name_lenght        AS INT)                    AS product_name_lenght_raw,
# MAGIC         CAST(product_description_lenght AS INT)                    AS product_description_lenght_raw,
# MAGIC         COALESCE(CAST(product_photos_qty AS INT), 1)               AS product_photos_qty,
# MAGIC         CAST(product_weight_g  AS FLOAT)                           AS product_weight_g,
# MAGIC         CAST(product_length_cm AS FLOAT)                           AS product_length_cm,
# MAGIC         CAST(product_height_cm AS FLOAT)                           AS product_height_cm,
# MAGIC         CAST(product_width_cm  AS FLOAT)                           AS product_width_cm
# MAGIC     FROM sales.bronze.products
# MAGIC     WHERE CAST(product_weight_g  AS FLOAT) > 0   AND CAST(product_weight_g  AS FLOAT) < 10000
# MAGIC       AND CAST(product_length_cm AS FLOAT) > 0   AND CAST(product_length_cm AS FLOAT) < 200
# MAGIC       AND CAST(product_height_cm AS FLOAT) > 0   AND CAST(product_height_cm AS FLOAT) < 200
# MAGIC       AND CAST(product_width_cm  AS FLOAT) > 0   AND CAST(product_width_cm  AS FLOAT) < 200
# MAGIC ),
# MAGIC imputed AS (
# MAGIC     SELECT *,
# MAGIC         COALESCE(product_name_lenght_raw,
# MAGIC             CAST(ROUND(AVG(product_name_lenght_raw) OVER (PARTITION BY product_category_name)) AS INT))
# MAGIC                                                                    AS product_name_lenght,
# MAGIC         COALESCE(product_description_lenght_raw,
# MAGIC             CAST(ROUND(AVG(product_description_lenght_raw) OVER (PARTITION BY product_category_name)) AS INT))
# MAGIC                                                                    AS product_description_lenght
# MAGIC     FROM base
# MAGIC )
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     product_category_name,
# MAGIC     product_name_lenght,
# MAGIC     product_description_lenght,
# MAGIC     product_photos_qty,
# MAGIC     product_weight_g,
# MAGIC     product_length_cm,
# MAGIC     product_height_cm,
# MAGIC     product_width_cm,
# MAGIC     ROUND(product_length_cm * product_height_cm * product_width_cm, 2)                         AS product_volume_cm3,
# MAGIC     CASE WHEN (product_length_cm * product_height_cm * product_width_cm) > 0
# MAGIC          THEN ROUND(product_weight_g / NULLIF(product_length_cm * product_height_cm * product_width_cm, 0), 4)
# MAGIC          ELSE NULL END                                                                          AS product_density_g_per_cm3,
# MAGIC     current_timestamp()                                                                         AS last_update_ts
# MAGIC FROM imputed

# COMMAND ----------

# DBTITLE 1,silver.orders
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.orders AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     CASE
# MAGIC         WHEN LOWER(TRIM(order_status)) IN ('delivered', 'invoiced')     THEN 'completed'
# MAGIC         WHEN LOWER(TRIM(order_status)) IN ('canceled', 'unavailable')   THEN 'canceled'
# MAGIC         ELSE 'pending'
# MAGIC     END                                                                  AS order_status_group,
# MAGIC     order_purchase_timestamp,
# MAGIC     order_delivered_customer_date,
# MAGIC     TO_DATE(order_purchase_timestamp)                                    AS purchase_date,
# MAGIC     TO_DATE(order_approved_at)                                           AS approved_date,
# MAGIC     TO_DATE(order_delivered_carrier_date)                                AS carrier_date,
# MAGIC     TO_DATE(order_delivered_customer_date)                               AS delivered_date,
# MAGIC     TO_DATE(order_estimated_delivery_date)                               AS estimated_date,
# MAGIC     HOUR(order_purchase_timestamp)                                       AS purchase_hour,
# MAGIC     CASE WHEN order_delivered_customer_date IS NOT NULL
# MAGIC          THEN HOUR(order_delivered_customer_date) END                    AS delivered_hour,
# MAGIC     DATE_FORMAT(order_purchase_timestamp, 'E')                           AS purchase_weekday_name,
# MAGIC     CASE WHEN order_delivered_customer_date IS NOT NULL
# MAGIC          THEN DATE_FORMAT(order_delivered_customer_date, 'E') END        AS delivered_weekday_name,
# MAGIC     DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)    AS delivery_days,
# MAGIC     DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) AS delay_days,
# MAGIC     CASE
# MAGIC         WHEN HOUR(order_purchase_timestamp) BETWEEN 6  AND 11 THEN 'morning'
# MAGIC         WHEN HOUR(order_purchase_timestamp) BETWEEN 12 AND 17 THEN 'afternoon'
# MAGIC         WHEN HOUR(order_purchase_timestamp) BETWEEN 18 AND 22 THEN 'evening'
# MAGIC         ELSE 'night'
# MAGIC     END                                                                  AS purchase_part_of_day,
# MAGIC     DAYOFWEEK(order_purchase_timestamp) IN (1, 7)                        AS is_weekend_purchase,
# MAGIC     (
# MAGIC         order_approved_at < order_purchase_timestamp
# MAGIC         OR order_delivered_carrier_date < order_approved_at
# MAGIC         OR order_delivered_customer_date < order_delivered_carrier_date
# MAGIC     )                                                                    AS is_invalid_timeflow,
# MAGIC     order_delivered_customer_date IS NULL                                AS is_missing_customer_date,
# MAGIC     order_delivered_carrier_date  IS NULL                                AS is_missing_carrier_date,
# MAGIC     order_approved_at             IS NULL                                AS is_missing_approved
# MAGIC FROM sales.bronze.orders

# COMMAND ----------

# DBTITLE 1,silver.order_items
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.order_items AS
# MAGIC WITH deduped AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY order_id, CAST(order_item_id AS INT)
# MAGIC             ORDER BY TO_TIMESTAMP(shipping_limit_date) DESC, CAST(price AS DOUBLE) DESC
# MAGIC         ) AS rn
# MAGIC     FROM sales.bronze.order_items
# MAGIC )
# MAGIC SELECT
# MAGIC     TRIM(order_id)                                                   AS order_id,
# MAGIC     CAST(order_item_id AS INT)                                       AS order_item_id,
# MAGIC     TRIM(product_id)                                                 AS product_id,
# MAGIC     TRIM(seller_id)                                                  AS seller_id,
# MAGIC     TO_TIMESTAMP(shipping_limit_date)                                AS shipping_limit_date,
# MAGIC     CAST(price         AS DOUBLE)                                    AS price,
# MAGIC     CAST(freight_value AS DOUBLE)                                    AS freight_value,
# MAGIC     CAST(price AS DOUBLE) <= 0                                       AS is_price_nonpositive,
# MAGIC     CAST(freight_value AS DOUBLE) < 0                                AS is_freight_negative,
# MAGIC     YEAR(TO_TIMESTAMP(shipping_limit_date)) < 2017
# MAGIC         OR YEAR(TO_TIMESTAMP(shipping_limit_date)) > 2019            AS is_ship_date_outlier,
# MAGIC     ROUND(CAST(price AS DOUBLE) + CAST(freight_value AS DOUBLE), 2) AS total_item_value,
# MAGIC     CASE WHEN CAST(price AS DOUBLE) > 0
# MAGIC          THEN ROUND(CAST(freight_value AS DOUBLE) / CAST(price AS DOUBLE), 3)
# MAGIC          ELSE NULL END                                               AS freight_ratio,
# MAGIC     YEAR(TO_TIMESTAMP(shipping_limit_date))                          AS ship_year,
# MAGIC     MONTH(TO_TIMESTAMP(shipping_limit_date))                         AS ship_month,
# MAGIC     DAYOFMONTH(TO_TIMESTAMP(shipping_limit_date))                    AS ship_day
# MAGIC FROM deduped
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,silver.order_payments
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.order_payments AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     TRY_CAST(payment_sequential   AS INT)                            AS payment_sequential,
# MAGIC     LOWER(TRIM(payment_type))                                        AS payment_type,
# MAGIC     TRY_CAST(payment_installments AS INT)                            AS payment_installments,
# MAGIC     ROUND(TRY_CAST(payment_value  AS DOUBLE), 2)                    AS payment_value,
# MAGIC     CASE
# MAGIC         WHEN TRY_CAST(payment_installments AS INT) > 0
# MAGIC          AND TRY_CAST(payment_value AS DOUBLE) IS NOT NULL
# MAGIC         THEN ROUND(TRY_CAST(payment_value AS DOUBLE) / TRY_CAST(payment_installments AS INT), 2)
# MAGIC         ELSE NULL
# MAGIC     END                                                              AS payment_per_installment
# MAGIC FROM sales.bronze.order_payments

# COMMAND ----------

# DBTITLE 1,silver.order_reviews (valid rows)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.order_reviews AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY review_id
# MAGIC             ORDER BY TRY_CAST(review_answer_timestamp AS TIMESTAMP) DESC,
# MAGIC                      TRY_CAST(review_creation_date    AS TIMESTAMP) DESC
# MAGIC         ) AS rn
# MAGIC     FROM sales.bronze.order_reviews
# MAGIC     WHERE review_id IS NOT NULL
# MAGIC       AND TRY_CAST(review_score          AS INT)       BETWEEN 1 AND 5
# MAGIC       AND TRY_CAST(review_creation_date  AS TIMESTAMP) IS NOT NULL
# MAGIC       AND TRY_CAST(review_answer_timestamp AS TIMESTAMP) IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     TRIM(review_id)                                                          AS review_id,
# MAGIC     TRIM(order_id)                                                           AS order_id,
# MAGIC     TRY_CAST(review_score AS INT)                                            AS review_score,
# MAGIC     TRY_CAST(review_creation_date    AS TIMESTAMP)                           AS review_creation_ts,
# MAGIC     TRY_CAST(review_answer_timestamp AS TIMESTAMP)                           AS review_answer_ts,
# MAGIC     CASE WHEN review_comment_title IS NULL AND review_comment_message IS NOT NULL
# MAGIC          THEN SUBSTRING(review_comment_message, 1, 50)
# MAGIC          ELSE TRIM(review_comment_title) END                                 AS review_comment_title,
# MAGIC     (review_comment_title IS NULL AND review_comment_message IS NULL)        AS is_missing_comment,
# MAGIC     (review_comment_title IS NOT NULL AND review_comment_message IS NULL)    AS has_title_only,
# MAGIC     (review_comment_title IS NULL AND review_comment_message IS NOT NULL)    AS has_message_only
# MAGIC FROM ranked
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,silver.reviews_bad_rows (quarantine)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.reviews_bad_rows AS
# MAGIC SELECT *
# MAGIC FROM sales.bronze.order_reviews
# MAGIC WHERE review_id IS NULL
# MAGIC    OR TRY_CAST(review_score AS INT) NOT BETWEEN 1 AND 5
# MAGIC    OR TRY_CAST(review_creation_date    AS TIMESTAMP) IS NULL
# MAGIC    OR TRY_CAST(review_answer_timestamp AS TIMESTAMP) IS NULL

# COMMAND ----------

# DBTITLE 1,silver.geolocation
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.geolocation AS
# MAGIC WITH normalized AS (
# MAGIC     SELECT
# MAGIC         CAST(geolocation_zip_code_prefix AS INT)                    AS zip_prefix,
# MAGIC         CAST(geolocation_lat AS DOUBLE)                             AS lat,
# MAGIC         CAST(geolocation_lng AS DOUBLE)                             AS lng,
# MAGIC         LOWER(TRIM(REGEXP_REPLACE(geolocation_city,  '\\s+', ' '))) AS city_clean,
# MAGIC         UPPER(TRIM(REGEXP_REPLACE(geolocation_state, '\\s+', ' '))) AS state_clean
# MAGIC     FROM sales.bronze.geolocation
# MAGIC ),
# MAGIC centroid AS (
# MAGIC     SELECT
# MAGIC         zip_prefix,
# MAGIC         ROUND(AVG(lat), 6) AS geolocation_lat,
# MAGIC         ROUND(AVG(lng), 6) AS geolocation_lng
# MAGIC     FROM normalized
# MAGIC     GROUP BY zip_prefix
# MAGIC ),
# MAGIC mode_city AS (
# MAGIC     SELECT zip_prefix, city_clean AS geolocation_city, state_clean AS geolocation_state
# MAGIC     FROM (
# MAGIC         SELECT zip_prefix, city_clean, state_clean,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY zip_prefix
# MAGIC                 ORDER BY COUNT(*) DESC, city_clean ASC
# MAGIC             ) AS rn
# MAGIC         FROM normalized
# MAGIC         GROUP BY zip_prefix, city_clean, state_clean
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC     c.zip_prefix  AS geolocation_zip_code_prefix,
# MAGIC     c.geolocation_lat,
# MAGIC     c.geolocation_lng,
# MAGIC     m.geolocation_city,
# MAGIC     m.geolocation_state
# MAGIC FROM centroid c
# MAGIC JOIN mode_city m ON c.zip_prefix = m.zip_prefix

# COMMAND ----------

# DBTITLE 1,silver.category_lookup
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.silver.category_lookup AS
# MAGIC SELECT
# MAGIC     product_category_name         AS category_pt,
# MAGIC     product_category_name_english AS category_en
# MAGIC FROM sales.bronze.product_category_name_translation

# COMMAND ----------

# DBTITLE 1,Silver row counts verification
silver_tables = [
    "sales.silver.customers", "sales.silver.sellers", "sales.silver.products",
    "sales.silver.orders", "sales.silver.order_items", "sales.silver.order_payments",
    "sales.silver.order_reviews", "sales.silver.reviews_bad_rows",
    "sales.silver.geolocation", "sales.silver.category_lookup",
]
print(f"{'Table':<50} {'Rows':>10}")
print("-" * 62)
for t in silver_tables:
    cnt = spark.table(t).count()
    print(f"{t:<50} {cnt:>10,}")