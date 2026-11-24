# Databricks notebook source
baseline_versions = {'workspace.olist_bronze.customers': 1, 'workspace.olist_bronze.geolocation': 1, 'workspace.olist_bronze.order_items': 1, 'workspace.olist_bronze.order_payments': 1, 'workspace.olist_bronze.order_reviews': 1, 'workspace.olist_bronze.orders': 3, 'workspace.olist_bronze.products': 1, 'workspace.olist_bronze.sellers': 1, 'workspace.olist_bronze.category_translation': 1, 'workspace.olist_silver.customers': 1, 'workspace.olist_silver.geolocation': 1, 'workspace.olist_silver.order_items': 1, 'workspace.olist_silver.order_payments': 1, 'workspace.olist_silver.order_reviews': 1, 'workspace.olist_silver.orders': 2, 'workspace.olist_silver.products': 1, 'workspace.olist_silver.sellers': 2, 'workspace.olist_silver.category_translation': 1, 'workspace.olist_gold.order_summary': 4, 'workspace.olist_gold.daily_sales': 1, 'workspace.olist_gold.product_performance': 1, 'workspace.olist_gold.customer_summary': 1, 'workspace.olist_gold.delivery_performance': 1}

replay_versions = {'workspace.olist_bronze.customers': 2, 'workspace.olist_bronze.geolocation': 2, 'workspace.olist_bronze.order_items': 2, 'workspace.olist_bronze.order_payments': 2, 'workspace.olist_bronze.order_reviews': 2, 'workspace.olist_bronze.orders': 5, 'workspace.olist_bronze.products': 2, 'workspace.olist_bronze.sellers': 2, 'workspace.olist_bronze.category_translation': 2, 'workspace.olist_silver.customers': 2, 'workspace.olist_silver.geolocation': 2, 'workspace.olist_silver.order_items': 2, 'workspace.olist_silver.order_payments': 2, 'workspace.olist_silver.order_reviews': 3, 'workspace.olist_silver.orders': 3, 'workspace.olist_silver.products': 2, 'workspace.olist_silver.sellers': 3, 'workspace.olist_silver.category_translation': 2, 'workspace.olist_gold.order_summary': 5, 'workspace.olist_gold.daily_sales': 2, 'workspace.olist_gold.product_performance': 2, 'workspace.olist_gold.customer_summary': 2, 'workspace.olist_gold.delivery_performance': 2}

# COMMAND ----------

assert baseline_versions.keys() == replay_versions.keys()
assert len(baseline_versions) == 23

results = []

for table, before_version in baseline_versions.items():
    after_version = replay_versions[table]
    assert after_version > before_version, (
        f"{table}: baseline={before_version}, replay={after_version}"
    )

    before = spark.sql(f"SELECT * FROM {table} VERSION AS OF {before_version}")
    after = spark.sql(f"SELECT * FROM {table} VERSION AS OF {after_version}")

    assert dict(before.dtypes) == dict(after.dtypes), f"Schema changed: {table}"

    columns = [name for name in before.columns if name != "_ingested_at"]
    before = before.select(*columns)
    after = after.select(*columns)

    removed = before.exceptAll(after).count()
    added = after.exceptAll(before).count()
    results.append((table, before_version, after_version, removed, added))

display(spark.createDataFrame(
    results,
    "table_name STRING, before_version LONG, after_version LONG, "
    "removed_rows LONG, added_rows LONG",
))

changed_tables = [table for table, _, _, removed, added in results if removed or added]
assert not changed_tables, f"Replay differences: {changed_tables}"

print(f"Replay verified across {len(results)} tables.")