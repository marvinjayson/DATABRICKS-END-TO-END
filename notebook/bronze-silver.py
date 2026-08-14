# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze → Silver
import yaml
from datetime import datetime

# ── Load SQL transforms from YAML ──────────────────────────────────────────
with open("/Volumes/sales/mapping/mappings/bronze_silver_transforms.yaml") as f:
    transforms = yaml.safe_load(f)["tables"]

# ── Execute each Bronze → Silver transform ─────────────────────────────────
print(f"Bronze -> Silver  |  {len(transforms)} tables\n")
print(f"  {'Table':<50} {'Rows':>10}  {'Time':>6}")
print(f"  {'-' * 70}")

for target, sql in transforms.items():
    t0 = datetime.now()
    try:
        spark.sql(sql)
        cnt     = spark.table(target).count()
        elapsed = round((datetime.now() - t0).total_seconds(), 1)
        print(f"  {target:<50} {cnt:>10,}  {elapsed:>5.1f}s")
    except Exception as e:
        print(f"  {target:<50} {'ERROR':>10}  {str(e)[:60]}")

print(f"\n  Done — {len(transforms)} tables written to sales.silver")