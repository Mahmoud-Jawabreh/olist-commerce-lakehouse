# Databricks notebook source
from pyspark.sql import functions as F

orders = spark.table("workspace.olist_bronze.orders")

TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

# COMMAND ----------

display(
    orders.groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

timestamp_checks = orders.select([
    F.sum(
        F.when(
            F.col(column).isNull()
            | (F.trim(F.col(column)) == ""),
            1,
        ).otherwise(0)
    ).alias(f"{column}_missing")
    for column in TIMESTAMP_COLUMNS
])

display(timestamp_checks)

# COMMAND ----------

display(
    orders.agg(
        F.count("*").alias("rows"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.sum(
            F.when(
                F.col("order_id").isNull()
                | (F.trim(F.col("order_id")) == ""),
                1,
            ).otherwise(0)
        ).alias("missing_order_ids"),
    )
)

# COMMAND ----------

def is_missing(column):
    return (
        F.col(column).isNull()
        | (F.trim(F.col(column)) == "")
    )


display(
    orders.groupBy("order_status")
    .agg(
        F.count("*").alias("orders"),
        *[
            F.sum(
                F.when(is_missing(column), 1).otherwise(0)
            ).alias(f"{column}_missing")
            for column in TIMESTAMP_COLUMNS
        ],
    )
    .orderBy(F.desc("orders"))
)

# COMMAND ----------

orders_checked = orders

for column in TIMESTAMP_COLUMNS:
    orders_checked = orders_checked.withColumn(
        f"{column}_parsed",
        F.expr(
            f"try_cast(nullif(trim(`{column}`), '') AS TIMESTAMP_NTZ)"
        ),
    )

# COMMAND ----------

parse_failure_summary = orders_checked.agg(*[
    F.sum(
        F.when(
            (~is_missing(column))
            & F.col(f"{column}_parsed").isNull(),
            1,
        ).otherwise(0)
    ).alias(f"{column}_parse_failures")
    for column in TIMESTAMP_COLUMNS
])

display(parse_failure_summary)

# COMMAND ----------

failure_counts = parse_failure_summary.first().asDict()

assert all(count == 0 for count in failure_counts.values()), (
    f"Timestamp parsing failed: {failure_counts}"
)

print("All nonblank timestamp values parsed successfully.")

# COMMAND ----------

orders_silver = orders_checked.select(*[
    (
        F.col(f"{column}_parsed").alias(column)
        if column in TIMESTAMP_COLUMNS
        else F.col(column)
    )
    for column in orders.columns
])

orders_silver = orders_silver.withColumn(
    "order_status",
    F.lower(F.trim(F.col("order_status"))),
)

# COMMAND ----------

quality_rules = {
    "_dq_delivered_missing_approval": (
        (F.col("order_status") == "delivered")
        & F.col("order_approved_at").isNull()
    ),
    "_dq_delivered_missing_carrier_date": (
        (F.col("order_status") == "delivered")
        & F.col("order_delivered_carrier_date").isNull()
    ),
    "_dq_delivered_missing_delivery_date": (
        (F.col("order_status") == "delivered")
        & F.col("order_delivered_customer_date").isNull()
    ),
    "_dq_canceled_with_delivery_date": (
        (F.col("order_status") == "canceled")
        & F.col("order_delivered_customer_date").isNotNull()
    ),
    "_dq_approval_before_purchase": (
        F.col("order_approved_at")
        < F.col("order_purchase_timestamp")
    ),
    "_dq_carrier_before_purchase": (
        F.col("order_delivered_carrier_date")
        < F.col("order_purchase_timestamp")
    ),
    "_dq_delivery_before_purchase": (
        F.col("order_delivered_customer_date")
        < F.col("order_purchase_timestamp")
    ),
    "_dq_delivery_before_carrier": (
        F.col("order_delivered_customer_date")
        < F.col("order_delivered_carrier_date")
    ),
}

for flag, condition in quality_rules.items():
    orders_silver = orders_silver.withColumn(
        flag,
        F.coalesce(condition, F.lit(False)),
    )

# COMMAND ----------

required_keys = ["order_id", "customer_id"]

key_checks = orders_silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("order_id").alias("distinct_order_ids"),
    *[
        F.sum(
            F.when(
                F.col(column).isNull()
                | (F.trim(F.col(column)) == ""),
                1,
            ).otherwise(0)
        ).alias(f"{column}_missing")
        for column in required_keys
    ],
).first().asDict()

assert key_checks["rows"] == 99_441, (
    f"Unexpected row count: {key_checks['rows']}"
)

assert key_checks["distinct_order_ids"] == key_checks["rows"], (
    "Duplicate or null order IDs detected"
)

for column in required_keys:
    assert key_checks[f"{column}_missing"] == 0, (
        f"Missing values detected in {column}"
    )

print("Order count, uniqueness, and required-key checks passed.")

# COMMAND ----------

TARGET_TABLE = "workspace.olist_silver.orders"

(
    orders_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

saved_orders = spark.table(TARGET_TABLE)

assert saved_orders.count() == 99_441

saved_orders.printSchema()
print(f"Successfully wrote {TARGET_TABLE}")