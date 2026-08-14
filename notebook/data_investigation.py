# Databricks notebook source
# DBTITLE 1,BRZ_REV_001 — Duplicate review_id Investigation
# ── BRZ_REV_001: Duplicate review_id deep-dive ───────────────────────────────
TBL = "sales.bronze.order_reviews"

# 1. Overall duplicate profile
spark.sql(f"""
  SELECT
    COUNT(*)                                        AS total_rows,
    COUNT(DISTINCT review_id)                       AS distinct_review_ids,
    COUNT(*) - COUNT(DISTINCT review_id)            AS duplicate_excess,
    ROUND((COUNT(*) - COUNT(DISTINCT review_id))
          / COUNT(*) * 100, 2)                      AS pct_dupes
  FROM {TBL}
""").show()

# 2. Are duplicates exact clones or diverging records?
spark.sql(f"""
  WITH dup_ids AS (
    SELECT review_id
    FROM   {TBL}
    GROUP  BY review_id
    HAVING COUNT(*) > 1
  )
  SELECT
    COUNT(*)                                        AS dup_review_ids,
    SUM(cnt)                                        AS total_dup_rows,
    MAX(cnt)                                        AS max_copies,
    ROUND(AVG(cnt), 2)                              AS avg_copies,
    SUM(CASE WHEN distinct_hashes = 1 THEN 1 ELSE 0 END)  AS exact_clones,
    SUM(CASE WHEN distinct_hashes > 1 THEN 1 ELSE 0 END)  AS diverging_records
  FROM (
    SELECT r.review_id,
           COUNT(*)                                  AS cnt,
           COUNT(DISTINCT CONCAT_WS('|',
             COALESCE(review_score,''),
             COALESCE(review_comment_title,''),
             COALESCE(review_comment_message,''),
             COALESCE(CAST(review_creation_date AS STRING),'')
           ))                                        AS distinct_hashes
    FROM   {TBL} r
    JOIN   dup_ids d USING (review_id)
    GROUP  BY r.review_id
  )
""").show()

# 3. Sample of diverging duplicates (different data on same review_id)
print("── Sample diverging duplicates ──────────────────────────────────────")
display(spark.sql(f"""
  WITH dup_ids AS (
    SELECT review_id
    FROM   {TBL}
    GROUP  BY review_id
    HAVING COUNT(*) > 1
  ),
  hash_counts AS (
    SELECT review_id,
           COUNT(DISTINCT CONCAT_WS('|',
             COALESCE(review_score,''),
             COALESCE(review_comment_title,''),
             COALESCE(review_comment_message,''),
             COALESCE(CAST(review_creation_date AS STRING),'')
           )) AS distinct_hashes
    FROM   {TBL}
    JOIN   dup_ids USING (review_id)
    GROUP  BY review_id
  )
  SELECT r.*, h.distinct_hashes
  FROM   {TBL} r
  JOIN   hash_counts h USING (review_id)
  WHERE  h.distinct_hashes > 1
  ORDER  BY review_id
  LIMIT  20
"""))

# 4. Duplicate volume by review_creation_date (month)
print("── Duplicate volume by review_creation_date (month) ────────────────")
display(spark.sql(f"""
  WITH dup_ids AS (
    SELECT review_id
    FROM   {TBL}
    GROUP  BY review_id
    HAVING COUNT(*) > 1
  )
  SELECT
    DATE_TRUNC('month', review_creation_date)       AS month,
    COUNT(*)                                        AS dup_rows
  FROM   {TBL} r
  JOIN   dup_ids d USING (review_id)
  GROUP  BY 1
  ORDER  BY 1
"""))

# COMMAND ----------

# DBTITLE 1,SLV_ORD_002 — Temporal Integrity Investigation
# ── SLV_ORD_002: Temporal integrity failures deep-dive ─────────────────────
# Schema: purchase_date, approved_date, carrier_date, delivered_date, estimated_date
# delay_days = delivered_date - estimated_date (negative = early, positive = late)
TBL = "sales.silver.orders"

# 1. Overall failure profile
spark.sql(f"""
  SELECT
    COUNT(*)                                              AS total_rows,
    SUM(CAST(is_invalid_timeflow AS INT))                 AS failing_rows,
    ROUND(SUM(CAST(is_invalid_timeflow AS INT))
          / COUNT(*) * 100, 2)                           AS pct_failing
  FROM {TBL}
""").show()

# 2. Which timestamp pair(s) are out of order?
# Lifecycle: purchase → approved → carrier → delivered | vs estimated
print("── Which timestamp pair violates order? ──────────────────────────")
spark.sql(f"""
  SELECT
    SUM(CASE WHEN approved_date < purchase_date      THEN 1 ELSE 0 END) AS approved_before_purchase,
    SUM(CASE WHEN carrier_date  < approved_date      THEN 1 ELSE 0 END) AS carrier_before_approved,
    SUM(CASE WHEN delivered_date < carrier_date      THEN 1 ELSE 0 END) AS delivered_before_carrier,
    SUM(CASE WHEN delivered_date > estimated_date    THEN 1 ELSE 0 END) AS delivered_after_estimated,
    SUM(CASE WHEN delivered_date < purchase_date     THEN 1 ELSE 0 END) AS delivered_before_purchase
  FROM {TBL}
  WHERE is_invalid_timeflow = true
""").show()

# 3. Distribution of lateness for the dominant violation (delivered > estimated)
print("── Gap size (days) for delivered_after_estimated ────────────────────")
spark.sql(f"""
  SELECT
    MIN(delay_days)               AS min_days_late,
    ROUND(AVG(delay_days), 1)     AS avg_days_late,
    MAX(delay_days)               AS max_days_late,
    PERCENTILE(delay_days, 0.5)   AS median_days_late,
    PERCENTILE(delay_days, 0.9)   AS p90_days_late
  FROM {TBL}
  WHERE is_invalid_timeflow = true
    AND delivered_date > estimated_date
""").show()

# 4. Trend: failure rate by purchase month
print("── Temporal failures by purchase month ────────────────────────────")
display(spark.sql(f"""
  SELECT
    DATE_TRUNC('month', purchase_date)                    AS purchase_month,
    COUNT(*)                                              AS total_orders,
    SUM(CAST(is_invalid_timeflow AS INT))                 AS failing,
    ROUND(SUM(CAST(is_invalid_timeflow AS INT))
          / COUNT(*) * 100, 2)                           AS pct_failing
  FROM {TBL}
  GROUP BY 1
  ORDER BY 1
"""))

# 5. Sample failing rows with all dates side by side
print("── Sample failing rows (worst delays first) ────────────────────────")
display(spark.sql(f"""
  SELECT
    order_id,
    order_status_group,
    purchase_date,
    approved_date,
    carrier_date,
    delivered_date,
    estimated_date,
    delay_days
  FROM {TBL}
  WHERE is_invalid_timeflow = true
  ORDER BY delay_days DESC
  LIMIT 20
"""))

# COMMAND ----------

