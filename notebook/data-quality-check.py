# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Data Quality Checks — All Layers
import yaml
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, DoubleType, TimestampType)

# ── Load DQ rules ────────────────────────────────────────────────────────────
with open("/Volumes/sales/mapping/mappings/dq_rules.yaml") as f:
    config = yaml.safe_load(f)

rules  = config["rules"]
run_ts = datetime.now()

def src_table(rule):
    return rule.get("source_table") or f"sales.{rule['layer'].lower()}.{rule['table'].lower()}"

# ── Pre-compute total counts once per unique table (parallel) ────────────────
# Eliminates redundant full-table scans when multiple rules share the same table
unique_tables = list({src_table(r) for r in rules})

def fetch_total(tbl):
    try:
        return tbl, spark.table(tbl).count()
    except Exception:
        return tbl, -1

with ThreadPoolExecutor(max_workers=min(8, len(unique_tables))) as pool:
    total_counts = dict(pool.map(fetch_total, unique_tables))

# ── Evaluate all rules in parallel ────────────────────────────────────────────
def run_rule(rule):
    src = src_table(rule)
    try:
        fail_cnt  = spark.sql(rule["fail_sql"]).count()
        total_cnt = total_counts.get(src, -1)
        pass_rate = round((1 - fail_cnt / max(total_cnt, 1)) * 100, 2)
        status    = ("PASS" if fail_cnt == 0
                     else "WARN" if rule["severity"] in ("Low", "Medium")
                     else "FAIL")
    except Exception as e:
        fail_cnt = total_cnt = -1
        pass_rate = 0.0
        status    = f"ERROR: {str(e)[:80]}"
    return (rule["rule_id"], rule["layer"], rule["table"], rule["rule_type"],
            rule["expectation"], rule["severity"],
            fail_cnt, total_cnt, pass_rate, status, run_ts)

with ThreadPoolExecutor(max_workers=min(16, len(rules))) as pool:
    results = list(pool.map(run_rule, rules))

# ── Write results to Delta table ──────────────────────────────────────────────
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
(dq_df.write.format("delta")
      .mode("append")
      .saveAsTable(config["output_table"]))

display(dq_df.orderBy("layer", "rule_id"))
# Use len(results) — avoids triggering an extra Spark count() action
print(f"\nDQ results saved to {config['output_table']}  ({len(results)} rules evaluated)")