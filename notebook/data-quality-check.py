# Databricks notebook source
# DBTITLE 1,Data Quality Checks — All Layers
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from datetime import datetime

results = []
run_ts  = datetime.now()

def dq_check(rule_id, layer, table, rule_type, expectation, severity, fail_sql, source_table=None):
    """Run a DQ rule: count failing rows, compute pass rate, derive status."""
    try:
        fail_cnt  = spark.sql(fail_sql).count()
        src       = source_table or f"sales.{layer.lower()}.{table.lower()}"
        total_cnt = spark.table(src).count()
        pass_rate = round((1 - fail_cnt / max(total_cnt, 1)) * 100, 2)
        status    = "PASS" if fail_cnt == 0 else ("WARN" if severity in ("Low", "Medium") else "FAIL")
    except Exception as e:
        fail_cnt = total_cnt = -1
        pass_rate = 0.0
        status    = "ERROR: " + str(e)[:80]
    results.append((rule_id, layer, table, rule_type, expectation, severity,
                    fail_cnt, total_cnt, pass_rate, status, run_ts))

# ── ORDERS ────────────────────────────────────────────────────────────────────
dq_check("BRZ_ORD_001", "BRONZE", "orders", "Duplicate check",
    "order_id should be unique", "Medium",
    "SELECT order_id FROM sales.bronze.orders GROUP BY order_id HAVING COUNT(*) > 1")

dq_check("SLV_ORD_001", "SILVER", "orders", "Standardization",
    "order_status_group must be one of: completed, canceled, pending", "High",
    "SELECT * FROM sales.silver.orders WHERE order_status_group NOT IN ('completed','canceled','pending')")

dq_check("SLV_ORD_002", "SILVER", "orders", "Temporal integrity",
    "Flag rows where is_invalid_timeflow = true", "High",
    "SELECT * FROM sales.silver.orders WHERE is_invalid_timeflow = true")

dq_check("SLV_ORD_003", "SILVER", "orders", "Completeness",
    "Orders missing approved / carrier / customer delivery dates", "Medium",
    "SELECT * FROM sales.silver.orders WHERE is_missing_approved OR is_missing_carrier_date OR is_missing_customer_date")

# ── ORDER REVIEWS ─────────────────────────────────────────────────────────────
dq_check("BRZ_REV_001", "BRONZE", "order_reviews", "Duplicate check",
    "Duplicate review_id in bronze", "High",
    "SELECT review_id FROM sales.bronze.order_reviews GROUP BY review_id HAVING COUNT(*) > 1")

dq_check("SLV_REV_001", "SILVER", "order_reviews", "Constraint",
    "review_score must be between 1 and 5", "High",
    "SELECT * FROM sales.silver.order_reviews WHERE review_score NOT BETWEEN 1 AND 5")

# SLV_REV_002 — quarantine count (bad_rows IS the failure set)
try:
    bad_cnt   = spark.table("sales.silver.reviews_bad_rows").count()
    total_rev = spark.table("sales.bronze.order_reviews").count()
    pass_rate = round((1 - bad_cnt / max(total_rev, 1)) * 100, 2)
    status    = "WARN" if bad_cnt > 0 else "PASS"
except Exception as e:
    bad_cnt = total_rev = -1; pass_rate = 0.0; status = "ERROR"
results.append(("SLV_REV_002", "SILVER", "reviews_bad_rows", "Quarantine",
    "Rows quarantined: null review_id, invalid score, or bad timestamps",
    "High", bad_cnt, total_rev, pass_rate, status, run_ts))

# ── PRODUCTS ──────────────────────────────────────────────────────────────────
dq_check("SLV_PRD_001", "SILVER", "products", "Range check",
    "Weight/dimension range violations after filter (expect 0)", "High",
    "SELECT * FROM sales.silver.products WHERE product_weight_g <= 0 OR product_weight_g >= 10000 "
    "OR product_length_cm <= 0 OR product_height_cm <= 0 OR product_width_cm <= 0")

dq_check("SLV_PRD_002", "SILVER", "products", "Missing value handling",
    "Null category or null photo count after COALESCE (expect 0)", "Medium",
    "SELECT * FROM sales.silver.products WHERE product_category_name IS NULL OR product_photos_qty IS NULL")

# ── ORDER ITEMS ───────────────────────────────────────────────────────────────
dq_check("SLV_ITM_001", "SILVER", "order_items", "Duplicate resolution",
    "Duplicate (order_id, order_item_id) composite keys (expect 0)", "High",
    "SELECT order_id, order_item_id FROM sales.silver.order_items "
    "GROUP BY order_id, order_item_id HAVING COUNT(*) > 1")

dq_check("SLV_ITM_002", "SILVER", "order_items", "Value check",
    "Rows with price <= 0 or freight_value < 0 (quality flags)", "Medium",
    "SELECT * FROM sales.silver.order_items WHERE is_price_nonpositive OR is_freight_negative")

dq_check("SLV_ITM_003", "SILVER", "order_items", "Date range check",
    "Ship year outliers outside 2017-2019", "Low",
    "SELECT * FROM sales.silver.order_items WHERE is_ship_date_outlier = true")

# ── GEOLOCATION ───────────────────────────────────────────────────────────────
dq_check("SLV_GEO_001", "SILVER", "geolocation", "Deduplication",
    "One row per zip prefix (expect 0 duplicates)", "Medium",
    "SELECT geolocation_zip_code_prefix FROM sales.silver.geolocation "
    "GROUP BY geolocation_zip_code_prefix HAVING COUNT(*) > 1")

# ── GOLD: ORDER PERFORMANCE ───────────────────────────────────────────────────
dq_check("GLD_ORDPERF_001", "GOLD", "order_performance", "Aggregation grain",
    "One row per order_id (expect 0 duplicates)", "High",
    "SELECT order_id FROM sales.gold.order_performance GROUP BY order_id HAVING COUNT(*) > 1",
    source_table="sales.gold.order_performance")

# ── Save results to sales.dq.dq_results ───────────────────────────────────────
schema = StructType([
    StructField("rule_id",     StringType(),    True),
    StructField("layer",       StringType(),    True),
    StructField("table_name",  StringType(),    True),
    StructField("rule_type",   StringType(),    True),
    StructField("expectation", StringType(),    True),
    StructField("severity",    StringType(),    True),
    StructField("fail_count",  LongType(),      True),
    StructField("total_count", LongType(),      True),
    StructField("pass_rate",   DoubleType(),    True),
    StructField("status",      StringType(),    True),
    StructField("run_ts",      TimestampType(), True),
])
dq_df = spark.createDataFrame(results, schema)
(
    dq_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sales.dq.dq_results")
)
display(dq_df.orderBy("layer", "rule_id"))
print(f"\nDQ results saved to sales.dq.dq_results  ({dq_df.count()} rules evaluated)")