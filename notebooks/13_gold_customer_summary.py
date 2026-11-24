# Databricks notebook source
from pyspark.sql import functions as F

orders = spark.table("workspace.olist_gold.order_summary")
delivered = F.col("order_status") == "delivered"

customer_summary = (
    orders.groupBy("customer_unique_id")
    .agg(
        F.count("*").alias("order_count"),
        F.count(F.when(delivered, 1)).alias("delivered_order_count"),
        F.min("purchase_date").alias("first_order_date"),
        F.max("purchase_date").alias("last_order_date"),
        F.sum(F.when(delivered, F.col("items_total"))).alias("items_total"),
        F.sum(F.when(delivered, F.col("freight_total"))).alias("freight_total"),
        F.sum(F.when(delivered, F.col("payment_total"))).alias("payment_total"),
        F.count(
            F.when(delivered & (F.col("payment_record_count") == 0), 1)
        ).alias("delivered_orders_without_payments"),
    )
    .withColumn("is_repeat_buyer", F.col("delivered_order_count") >= 2)
)

# COMMAND ----------

MEASURES = [
    "delivered_order_count",
    "items_total",
    "freight_total",
    "payment_total",
]

expected = (
    spark.table("workspace.olist_gold.daily_sales")
    .agg(*[F.sum(name).alias(name) for name in MEASURES])
    .first()
    .asDict()
)

def validate_customer_summary(df):
    actual = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("customer_unique_id").alias("distinct_customers"),
        F.sum("order_count").alias("total_orders"),
        F.count(F.when(F.col("is_repeat_buyer"), 1)).alias("repeat_buyers"),
        *[F.sum(name).alias(name) for name in MEASURES],
    ).first().asDict()

    assert actual["rows"] == actual["distinct_customers"] == 96_096, actual
    assert actual["total_orders"] == 99_441, actual

    for name, value in expected.items():
        assert actual[name] == value, (
            f"{name}: daily_sales = {value}, customer_summary = {actual[name]}"
        )

    return actual

validate_customer_summary(customer_summary)

TARGET_TABLE = "workspace.olist_gold.customer_summary"
customer_summary.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

saved_customers = spark.table(TARGET_TABLE)
print(validate_customer_summary(saved_customers))
display(saved_customers.orderBy(F.desc("delivered_order_count"), "customer_unique_id").limit(10))