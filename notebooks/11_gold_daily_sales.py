# Databricks notebook source
from pyspark.sql import functions as F

delivered_orders = (
    spark.table("workspace.olist_gold.order_summary")
    .filter(F.col("order_status") == "delivered")
)

assert delivered_orders.filter(F.col("purchase_date").isNull()).count() == 0, (
    "Delivered orders are missing purchase dates"
)

daily_sales = delivered_orders.groupBy("purchase_date").agg(
    F.count("*").alias("delivered_order_count"),
    F.sum("item_count").alias("item_count"),
    F.sum("items_total").alias("items_total"),
    F.sum("freight_total").alias("freight_total"),
    F.sum("payment_total").alias("payment_total"),
    F.count(F.when(F.col("item_count") == 0, 1))
        .alias("orders_without_items"),
    F.count(F.when(F.col("payment_record_count") == 0, 1))
        .alias("orders_without_payments"),
)

# COMMAND ----------

MEASURES = ["item_count", "items_total", "freight_total", "payment_total"]

expected = delivered_orders.agg(
    F.count("*").alias("delivered_order_count"),
    *[F.sum(name).alias(name) for name in MEASURES],
).first().asDict()

assert expected["delivered_order_count"] == 96_478, expected

def validate_daily_sales(df):
    actual = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("purchase_date").alias("distinct_dates"),
        *[F.sum(name).alias(name) for name in expected],
    ).first().asDict()

    assert actual["rows"] == actual["distinct_dates"], actual
    for name, value in expected.items():
        assert actual[name] == value, (
            f"{name}: delivered orders = {value}, daily sales = {actual[name]}"
        )

    return actual

validate_daily_sales(daily_sales)

TARGET_TABLE = "workspace.olist_gold.daily_sales"
daily_sales.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

saved_daily_sales = spark.table(TARGET_TABLE)
print(validate_daily_sales(saved_daily_sales))
display(saved_daily_sales.orderBy("purchase_date").limit(10))