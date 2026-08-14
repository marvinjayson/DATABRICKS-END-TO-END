# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Row Count Audit — Bronze / Silver / Gold
import yaml
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# ── Load table registry from YAML ─────────────────────────────────────────
with open("/Volumes/sales/mapping/mappings/audit_tables.yaml") as f:
    config = yaml.safe_load(f)

# ── Count rows for every table ─────────────────────────────────────────────
rows, run_ts = [], datetime.now()
for layer, tables in config["tables"].items():
    for tbl in tables:
        try:
            cnt, status = spark.table(tbl).count(), "OK"
        except Exception as e:
            cnt, status = -1, str(e)[:120]
        rows.append((layer, tbl, cnt, status, run_ts))

# ── Write to audit Delta table ──────────────────────────────────────────────
schema = StructType([
    StructField("layer",      StringType(),    True),
    StructField("table_name", StringType(),    True),
    StructField("row_count",  LongType(),      True),
    StructField("status",     StringType(),    True),
    StructField("audit_ts",   TimestampType(), True),
])
audit_df = spark.createDataFrame(rows, schema)
(
    audit_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config["output_table"])
)
display(audit_df.orderBy("layer", "table_name"))
print(f"\nAudit saved to {config['output_table']}  ({len(rows)} tables)")