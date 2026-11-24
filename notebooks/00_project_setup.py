# Databricks notebook source
display(spark.sql("SELECT current_catalog(), current_schema()"))

# COMMAND ----------

display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

CATALOG = "workspace"

for schema in [
    "olist_raw",
    "olist_bronze",
    "olist_silver",
    "olist_gold",
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql("""
    CREATE VOLUME IF NOT EXISTS workspace.olist_raw.source_files
""")

# COMMAND ----------

RAW_PATH = "/Volumes/workspace/olist_raw/source_files"

files = dbutils.fs.ls(RAW_PATH)

display(files)

csv_files = [
    file for file in files
    if file.name.lower().endswith(".csv")
]

print(f"CSV files found: {len(csv_files)}")