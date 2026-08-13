# Databricks notebook source
# DBTITLE 1,Row Count Audit — Bronze / Silver / Gold
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from datetime import datetime

BRONZE_TABLES = [
    "sales.bronze.customers",
    "sales.bronze.geolocation",
    "sales.bronze.order_items",
    "sales.bronze.order_payments",
    "sales.bronze.order_reviews",
    "sales.bronze.orders",
    "sales.bronze.product_category_name_translation",
    "sales.bronze.products",
    "sales.bronze.sellers",
]
SILVER_TABLES = [
    "sales.silver.customers",
    "sales.silver.sellers",
    "sales.silver.products",
    "sales.silver.orders",
    "sales.silver.order_items",
    "sales.silver.order_payments",
    "sales.silver.order_reviews",
    "sales.silver.reviews_bad_rows",
    "sales.silver.geolocation",
    "sales.silver.category_lookup",
]
GOLD_TABLES = [
    "sales.gold.customer_top_5_states_by_city_diversity",
    "sales.gold.customer_distribution_by_state",
    "sales.gold.seller_city_stats",
    "sales.gold.product_category_summary",
    "sales.gold.product_category_summary_en",
    "sales.gold.order_performance",
]

rows = []
run_ts = datetime.now()
for layer, tables in [("BRONZE", BRONZE_TABLES), ("SILVER", SILVER_TABLES), ("GOLD", GOLD_TABLES)]:
    for tbl in tables:
        try:
            cnt    = spark.table(tbl).count()
            status = "OK"
        except Exception as e:
            cnt    = -1
            status = str(e)[:120]
        rows.append((layer, tbl, cnt, status, run_ts))

schema = StructType([
    StructField("layer",      StringType(),    True),
    StructField("table_name", StringType(),    True),
    StructField("row_count",  LongType(),      True),
    StructField("status",     StringType(),    True),
    StructField("audit_ts",   TimestampType(), True),
])
audit_df = spark.createDataFrame(rows, schema)

(
    audit_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sales.audit.row_count_audit")
)

display(audit_df.orderBy("layer", "table_name"))
print(f"\nAudit saved to sales.audit.row_count_audit  ({len(rows)} tables)")