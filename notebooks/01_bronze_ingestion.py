# Databricks notebook source
from pyspark.sql import functions as F

RAW_PATH = "/Volumes/workspace/olist_raw/source_files"
BRONZE_SCHEMA = "workspace.olist_bronze"

# COMMAND ----------

orders_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "FAILFAST")
    .csv(f"{RAW_PATH}/olist_orders_dataset.csv")
)

# COMMAND ----------

orders_bronze = (
    orders_raw
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
)


# COMMAND ----------

(
    orders_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{BRONZE_SCHEMA}.orders")
)

# COMMAND ----------

saved_orders = spark.table(f"{BRONZE_SCHEMA}.orders")

saved_orders.printSchema()

order_count = saved_orders.count()
print(f"Bronze orders: {order_count:,}")

assert order_count == 99_441, (
    f"Expected 99,441 orders, found {order_count:,}"
)

display(saved_orders.limit(5))

# COMMAND ----------

SOURCE_FILES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# COMMAND ----------

def ingest_bronze(table_name, filename):
    source_path = f"{RAW_PATH}/{filename}"
    target_table = f"{BRONZE_SCHEMA}.{table_name}"

    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(source_path)
    )

    bronze_df = (
        raw_df
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    row_count = spark.table(target_table).count()
    print(f"{target_table}: {row_count:,} rows")

    return table_name, filename, row_count

# COMMAND ----------

ingestion_results = []

for table_name, filename in SOURCE_FILES.items():
    result = ingest_bronze(table_name, filename)
    ingestion_results.append(result)

summary_df = spark.createDataFrame(
    ingestion_results,
    schema="table_name STRING, source_file STRING, row_count LONG",
)

display(summary_df.orderBy("table_name"))

# COMMAND ----------

EXPECTED_COUNTS = {
    "orders": 99_441,
    "customers": 99_441,
    "order_items": 112_650,
    "order_payments": 103_886,
    "order_reviews": 99_224,
    "products": 32_951,
    "sellers": 3_095,
}

actual_counts = {
    table_name: row_count
    for table_name, _, row_count in ingestion_results
}

for table_name, expected in EXPECTED_COUNTS.items():
    actual = actual_counts[table_name]

    assert actual == expected, (
        f"{table_name}: expected {expected:,}, found {actual:,}"
    )

print("All seven established row counts match.")