# Databricks notebook source
# DBTITLE 1,Silver → Gold
# MAGIC %md
# MAGIC # Silver → Gold
# MAGIC Builds analytical aggregates and marts in `sales.gold.*` from clean `sales.silver.*` tables.
# MAGIC Run all cells top-to-bottom.

# COMMAND ----------

# DBTITLE 1,gold.customer_top_5_states_by_city_diversity
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.gold.customer_top_5_states_by_city_diversity AS
# MAGIC SELECT
# MAGIC     customer_state                   AS state,
# MAGIC     COUNT(DISTINCT customer_city)    AS distinct_cities
# MAGIC FROM sales.silver.customers
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY distinct_cities DESC
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,gold.customer_distribution_by_state
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.gold.customer_distribution_by_state AS
# MAGIC WITH city_counts AS (
# MAGIC     SELECT
# MAGIC         customer_state,
# MAGIC         customer_city,
# MAGIC         COUNT(*) AS customer_count
# MAGIC     FROM sales.silver.customers
# MAGIC     GROUP BY customer_state, customer_city
# MAGIC ),
# MAGIC ranked_cities AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY customer_state ORDER BY customer_count DESC) AS rn
# MAGIC     FROM city_counts
# MAGIC )
# MAGIC SELECT
# MAGIC     c.customer_state                                     AS state,
# MAGIC     COUNT(DISTINCT c.customer_city)                      AS distinct_cities,
# MAGIC     SUM(c.customer_count)                                AS total_customers,
# MAGIC     ROUND(AVG(c.customer_count), 2)                      AS avg_customers_per_city,
# MAGIC     MAX(CASE WHEN r.rn = 1 THEN r.customer_city  END)   AS top_city,
# MAGIC     MAX(CASE WHEN r.rn = 1 THEN r.customer_count END)   AS top_city_customers
# MAGIC FROM city_counts c
# MAGIC JOIN ranked_cities r
# MAGIC   ON c.customer_state = r.customer_state
# MAGIC  AND c.customer_city  = r.customer_city
# MAGIC GROUP BY c.customer_state
# MAGIC ORDER BY total_customers DESC

# COMMAND ----------

# DBTITLE 1,gold.seller_city_stats
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.gold.seller_city_stats AS
# MAGIC WITH city_agg AS (
# MAGIC     SELECT
# MAGIC         INITCAP(seller_city)  AS city_display,
# MAGIC         UPPER(seller_state)   AS state_code,
# MAGIC         COUNT(*)              AS seller_count
# MAGIC     FROM sales.silver.sellers
# MAGIC     GROUP BY INITCAP(seller_city), UPPER(seller_state)
# MAGIC ),
# MAGIC total AS (
# MAGIC     SELECT SUM(seller_count) AS grand_total FROM city_agg
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT *,
# MAGIC         DENSE_RANK() OVER (ORDER BY seller_count DESC, city_display) AS rnk
# MAGIC     FROM city_agg
# MAGIC )
# MAGIC SELECT
# MAGIC     r.city_display,
# MAGIC     r.state_code,
# MAGIC     r.seller_count,
# MAGIC     r.rnk,
# MAGIC     ROUND(
# MAGIC         SUM(r.seller_count) OVER (ORDER BY r.rnk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
# MAGIC         / t.grand_total, 3
# MAGIC     ) AS cum_share
# MAGIC FROM ranked r
# MAGIC CROSS JOIN total t
# MAGIC ORDER BY r.rnk

# COMMAND ----------

# DBTITLE 1,gold.product_category_summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.gold.product_category_summary AS
# MAGIC SELECT
# MAGIC     product_category_name,
# MAGIC     ROUND(AVG(product_weight_g),    2) AS avg_weight_g,
# MAGIC     ROUND(AVG(product_length_cm),   2) AS avg_length_cm,
# MAGIC     ROUND(AVG(product_height_cm),   2) AS avg_height_cm,
# MAGIC     ROUND(AVG(product_width_cm),    2) AS avg_width_cm,
# MAGIC     ROUND(AVG(product_volume_cm3),  2) AS avg_volume_cm3,
# MAGIC     ROUND(AVG(product_weight_g / NULLIF(product_volume_cm3, 0)), 3) AS avg_density
# MAGIC FROM sales.silver.products
# MAGIC WHERE product_category_name IS NOT NULL
# MAGIC GROUP BY product_category_name
# MAGIC ORDER BY product_category_name

# COMMAND ----------

# DBTITLE 1,gold.product_category_summary_en
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.gold.product_category_summary_en AS
# MAGIC SELECT
# MAGIC     p.product_category_name                                                            AS category_pt,
# MAGIC     COALESCE(l.category_en, p.product_category_name)                                  AS category_en,
# MAGIC     INITCAP(REGEXP_REPLACE(COALESCE(l.category_en, p.product_category_name), '_', ' ')) AS category_en_display,
# MAGIC     p.avg_weight_g,
# MAGIC     p.avg_length_cm,
# MAGIC     p.avg_height_cm,
# MAGIC     p.avg_width_cm,
# MAGIC     p.avg_volume_cm3,
# MAGIC     p.avg_density
# MAGIC FROM sales.gold.product_category_summary p
# MAGIC LEFT JOIN sales.silver.category_lookup l ON p.product_category_name = l.category_pt
# MAGIC ORDER BY category_en_display

# COMMAND ----------

# DBTITLE 1,gold.order_performance
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales.gold.order_performance AS
# MAGIC WITH item_summary AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         ROUND(SUM(price + freight_value), 2) AS order_value,
# MAGIC         COUNT(*)                              AS item_count
# MAGIC     FROM sales.silver.order_items
# MAGIC     GROUP BY order_id
# MAGIC ),
# MAGIC payment_summary AS (
# MAGIC     SELECT DISTINCT
# MAGIC         order_id,
# MAGIC         FIRST_VALUE(payment_type) OVER (
# MAGIC             PARTITION BY order_id ORDER BY payment_sequential
# MAGIC         )                                                       AS payment_type,
# MAGIC         ROUND(SUM(payment_value) OVER (PARTITION BY order_id), 2) AS total_payment,
# MAGIC         ROUND(AVG(NULLIF(payment_installments, 0)) OVER (PARTITION BY order_id), 2) AS avg_installments
# MAGIC     FROM sales.silver.order_payments
# MAGIC ),
# MAGIC reviews_summary AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         ROUND(AVG(review_score), 2) AS avg_review_score
# MAGIC     FROM sales.silver.order_reviews
# MAGIC     GROUP BY order_id
# MAGIC )
# MAGIC SELECT
# MAGIC     o.order_id,
# MAGIC     DATE(o.order_purchase_timestamp)   AS purchase_date,
# MAGIC     o.order_status_group,
# MAGIC     o.delivery_days,
# MAGIC     o.delay_days,
# MAGIC     i.order_value,
# MAGIC     i.item_count,
# MAGIC     p.payment_type,
# MAGIC     p.total_payment,
# MAGIC     p.avg_installments,
# MAGIC     r.avg_review_score
# MAGIC FROM sales.silver.orders o
# MAGIC LEFT JOIN item_summary    i ON o.order_id = i.order_id
# MAGIC LEFT JOIN payment_summary p ON o.order_id = p.order_id
# MAGIC LEFT JOIN reviews_summary r ON o.order_id = r.order_id

# COMMAND ----------

# DBTITLE 1,Gold row counts verification
gold_tables = [
    "sales.gold.customer_top_5_states_by_city_diversity",
    "sales.gold.customer_distribution_by_state",
    "sales.gold.seller_city_stats",
    "sales.gold.product_category_summary",
    "sales.gold.product_category_summary_en",
    "sales.gold.order_performance",
]
print(f"{'Table':<60} {'Rows':>10}")
print("-" * 72)
for t in gold_tables:
    cnt = spark.table(t).count()
    print(f"{t:<60} {cnt:>10,}")