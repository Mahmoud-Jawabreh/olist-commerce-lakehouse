# Databricks notebook source
from pyspark.sql import functions as F

delivered_order_ids = (
    spark.table("workspace.olist_gold.order_summary")
    .filter(F.col("order_status") == "delivered")
    .select("order_id")
)

delivered_items = spark.table("workspace.olist_silver.order_items").join(
    delivered_order_ids, on="order_id", how="left_semi"
)

product_totals = delivered_items.groupBy("product_id").agg(
    F.count("*").alias("item_count"),
    F.countDistinct("order_id").alias("delivered_order_count"),
    F.sum("price").alias("items_total"),
    F.sum("freight_value").alias("freight_total"),
)

products = spark.table("workspace.olist_silver.products").select(
    "product_id",
    "product_category_name",
    "_dq_missing_category",
    "_dq_untranslated_category",
)

translations = spark.table("workspace.olist_silver.category_translation").select(
    "product_category_name",
    "product_category_name_english",
)

product_performance = (
    product_totals
    .join(products, on="product_id", how="left")
    .join(translations, on="product_category_name", how="left")
)

# COMMAND ----------

MEASURES = ["item_count", "items_total", "freight_total"]

expected = spark.table("workspace.olist_gold.daily_sales").agg(
    *[F.sum(name).alias(name) for name in MEASURES]
).first().asDict()

def validate_product_performance(df):
    actual = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.count(F.when(F.col("_dq_missing_category").isNull(), 1))
            .alias("missing_product_matches"),
        *[F.sum(name).alias(name) for name in MEASURES],
    ).first().asDict()

    assert actual["rows"] == actual["distinct_products"], actual
    assert actual["missing_product_matches"] == 0, actual

    for name in MEASURES:
        assert actual[name] == expected[name], (
            f"{name}: daily_sales = {expected[name]}, "
            f"product_performance = {actual[name]}"
        )

    return actual

validate_product_performance(product_performance)

TARGET_TABLE = "workspace.olist_gold.product_performance"
product_performance.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

saved_products = spark.table(TARGET_TABLE)
print(validate_product_performance(saved_products))
display(saved_products.orderBy(F.desc("items_total"), "product_id").limit(10))