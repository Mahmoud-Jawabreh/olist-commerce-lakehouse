# Databricks notebook source
import json
from pyspark.sql import functions as F

dbutils.widgets.text("job_run_id", "")
run_id = dbutils.widgets.get("job_run_id")
assert run_id.isdigit(), "Run this notebook through the job with job_run_id."

expected_rows = {
    "order_summary": 99_441,
    "daily_sales": 612,
    "product_performance": 32_216,
    "customer_summary": 96_096,
    "delivery_performance": 96_478,
}

audit_rows = []
for name, expected_count in expected_rows.items():
    table_name = f"workspace.olist_gold.{name}"
    table = spark.table(table_name)
    flags = [column for column in table.columns if column.startswith("_dq_")]

    metrics = table.agg(
        F.count("*").alias("row_count"),
        *[
            F.count(F.when(F.col(flag), 1)).alias(flag)
            for flag in flags
        ],
    ).first().asDict()

    row_count = metrics.pop("row_count")
    assert row_count == expected_count, (
        f"{table_name}: expected {expected_count}, found {row_count}"
    )
    audit_rows.append((run_id, table_name, row_count, json.dumps(metrics)))

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.olist_audit")
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.olist_audit.gold_run_metrics (
        job_run_id STRING,
        table_name STRING,
        row_count BIGINT,
        flag_counts_json STRING,
        checked_at TIMESTAMP
    ) USING DELTA
""")

audit = spark.createDataFrame(
    audit_rows,
    "job_run_id STRING, table_name STRING, "
    "row_count LONG, flag_counts_json STRING",
).withColumn("checked_at", F.current_timestamp())

audit.createOrReplaceTempView("olist_gold_audit_current_run")

spark.sql("""
    MERGE INTO workspace.olist_audit.gold_run_metrics AS target
    USING olist_gold_audit_current_run AS source
    ON target.job_run_id = source.job_run_id
       AND target.table_name = source.table_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

saved = spark.table("workspace.olist_audit.gold_run_metrics").where(
    F.col("job_run_id") == run_id
)
assert saved.count() == saved.select("table_name").distinct().count() == 5
display(saved.orderBy("table_name"))