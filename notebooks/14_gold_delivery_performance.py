# Databricks notebook source
from pyspark.sql import functions as F

purchase = F.col("order_purchase_timestamp")
actual = F.col("order_delivered_customer_date")
estimated = F.col("order_estimated_delivery_date")

delivery_performance = (
    spark.table("workspace.olist_gold.order_summary")
    .filter(F.col("order_status") == "delivered")
    .select(
        "order_id",
        "purchase_date",
        "customer_state",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "_dq_delivered_missing_delivery_date",
        "_dq_carrier_before_purchase",
        "_dq_delivery_before_carrier",
    )
    .withColumn("actual_delivery_date", F.to_date(actual))
    .withColumn("estimated_delivery_date", F.to_date(estimated))
    .withColumn(
        "delivery_duration_eligible",
        F.coalesce(actual >= purchase, F.lit(False)),
    )
    .withColumn(
        "on_time_eligible",
        F.col("delivery_duration_eligible") & estimated.isNotNull(),
    )
    .withColumn(
        "delivery_days",
        F.when(
            F.col("delivery_duration_eligible"),
            F.datediff("actual_delivery_date", "purchase_date"),
        ),
    )
    .withColumn(
        "delivery_days_vs_estimate",
        F.when(
            F.col("on_time_eligible"),
            F.datediff("actual_delivery_date", "estimated_delivery_date"),
        ),
    )
    .withColumn("is_on_time", F.col("delivery_days_vs_estimate") <= 0)
)

# COMMAND ----------

def validate_delivery_performance(df):
    checks = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("order_id").alias("distinct_orders"),
        F.count("delivery_days").alias("duration_eligible"),
        F.count("is_on_time").alias("on_time_eligible"),
        F.count(F.when(F.col("is_on_time"), 1)).alias("on_time_orders"),
        F.count(F.when(~F.col("is_on_time"), 1)).alias("late_orders"),
        F.count(F.when(F.col("delivery_days") < 0, 1))
            .alias("negative_durations"),
    ).first().asDict()

    assert checks["rows"] == checks["distinct_orders"] == 96_478, checks
    assert checks["negative_durations"] == 0, checks
    assert checks["on_time_orders"] + checks["late_orders"] == checks["on_time_eligible"], checks
    assert checks["on_time_eligible"] <= checks["duration_eligible"], checks
    return checks

expected = validate_delivery_performance(delivery_performance)

TARGET_TABLE = "workspace.olist_gold.delivery_performance"
delivery_performance.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

saved_checks = validate_delivery_performance(spark.table(TARGET_TABLE))
assert saved_checks == expected, saved_checks

print(saved_checks)
print(f"Successfully wrote and verified {TARGET_TABLE}")