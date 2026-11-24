# Databricks notebook source
from pyspark.sql import functions as F

payments = spark.table("workspace.olist_bronze.order_payments")

PAYMENT_COLUMNS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]

display(
    payments.agg(
        F.count("*").alias("rows"),
        F.countDistinct(
            "order_id", "payment_sequential"
        ).alias("distinct_payment_keys"),
        *[
            F.sum(
                F.when(
                    F.col(column).isNull()
                    | (F.trim(F.col(column)) == ""),
                    1,
                ).otherwise(0)
            ).alias(f"{column}_missing")
            for column in PAYMENT_COLUMNS
        ],
    )
)

# COMMAND ----------

TARGET_TYPES = {
    "payment_sequential": "INT",
    "payment_installments": "INT",
    "payment_value": "DECIMAL(18,2)",
}

payments_checked = payments

for column, target_type in TARGET_TYPES.items():
    payments_checked = payments_checked.withColumn(
        f"{column}_parsed",
        F.expr(
            f"try_cast(nullif(trim(`{column}`), '') AS {target_type})"
        ),
    )

# COMMAND ----------

display(
    payments_checked.agg(
        *[
            F.sum(
                F.when(
                    F.col(column).isNotNull()
                    & (F.trim(F.col(column)) != "")
                    & F.col(f"{column}_parsed").isNull(),
                    1,
                ).otherwise(0)
            ).alias(f"{column}_parse_failures")
            for column in TARGET_TYPES
        ],
        F.sum(
            F.when(
                F.col("payment_value").isNotNull()
                & (F.trim(F.col("payment_value")) != "")
                & ~F.trim(F.col("payment_value")).rlike(
                    r"^[+-]?\d+(\.\d{1,2})?$"
                ),
                1,
            ).otherwise(0)
        ).alias("payment_value_unexpected_format"),
    )
)

# COMMAND ----------

display(
    payments_checked
    .groupBy(
        F.lower(F.trim(F.col("payment_type"))).alias("payment_type")
    )
    .agg(
        F.count("*").alias("rows"),

        F.min("payment_sequential_parsed").alias("min_sequence"),
        F.max("payment_sequential_parsed").alias("max_sequence"),

        F.min("payment_installments_parsed").alias("min_installments"),
        F.max("payment_installments_parsed").alias("max_installments"),

        F.min("payment_value_parsed").alias("min_value"),
        F.max("payment_value_parsed").alias("max_value"),

        F.sum(
            F.when(
                F.col("payment_sequential_parsed") <= 0, 1
            ).otherwise(0)
        ).alias("nonpositive_sequences"),

        F.sum(
            F.when(
                F.col("payment_installments_parsed") < 0, 1
            ).otherwise(0)
        ).alias("negative_installments"),

        F.sum(
            F.when(
                F.col("payment_installments_parsed") == 0, 1
            ).otherwise(0)
        ).alias("zero_installments"),

        F.sum(
            F.when(
                F.col("payment_value_parsed") < 0, 1
            ).otherwise(0)
        ).alias("negative_values"),

        F.sum(
            F.when(
                F.col("payment_value_parsed") == 0, 1
            ).otherwise(0)
        ).alias("zero_values"),
    )
    .orderBy(F.desc("rows"))
)

# COMMAND ----------

payments_silver = payments_checked.select(*[
    (
        F.col(f"{column}_parsed").alias(column)
        if column in TARGET_TYPES
        else F.col(column)
    )
    for column in payments.columns
])

payments_silver = (
    payments_silver
    .withColumn(
        "payment_type",
        F.lower(F.trim(F.col("payment_type"))),
    )
    .withColumn(
        "_dq_zero_installments",
        F.col("payment_installments") == 0,
    )
    .withColumn(
        "_dq_zero_payment_value",
        F.col("payment_value") == 0,
    )
    .withColumn(
        "_dq_undefined_payment_type",
        F.col("payment_type") == "not_defined",
    )
)

# COMMAND ----------

checks = payments_silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct(
        "order_id", "payment_sequential"
    ).alias("distinct_payment_keys"),

    *[
        F.sum(
            F.when(
                F.col(column).isNull()
                | (F.trim(F.col(column).cast("string")) == ""),
                1,
            ).otherwise(0)
        ).alias(f"{column}_missing")
        for column in PAYMENT_COLUMNS
    ],

    F.sum(
        F.when(F.col("payment_sequential") <= 0, 1).otherwise(0)
    ).alias("nonpositive_sequences"),

    F.sum(
        F.when(F.col("payment_installments") < 0, 1).otherwise(0)
    ).alias("negative_installments"),

    F.sum(
        F.when(F.col("payment_value") < 0, 1).otherwise(0)
    ).alias("negative_values"),
).first().asDict()

assert checks["rows"] == 103_886
assert checks["distinct_payment_keys"] == checks["rows"]

for column in PAYMENT_COLUMNS:
    assert checks[f"{column}_missing"] == 0, (
        f"Missing or unparseable values: {column}"
    )

for rule in [
    "nonpositive_sequences",
    "negative_installments",
    "negative_values",
]:
    assert checks[rule] == 0, f"Quality check failed: {rule}"

# COMMAND ----------

silver_order_ids = (
    spark.table("workspace.olist_silver.orders")
    .select("order_id")
)

orphan_count = payments_silver.join(
    silver_order_ids,
    on="order_id",
    how="left_anti",
).count()

assert orphan_count == 0, (
    f"{orphan_count:,} payments reference missing Silver orders"
)

print("Payment publication checks passed.")

# COMMAND ----------

FLAGS = [
    "_dq_zero_installments",
    "_dq_zero_payment_value",
    "_dq_undefined_payment_type",
]

display(
    payments_silver.agg(*[
        F.sum(F.col(flag).cast("long")).alias(flag)
        for flag in FLAGS
    ])
)

TARGET_TABLE = "workspace.olist_silver.order_payments"

(
    payments_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

saved_payments = spark.table(TARGET_TABLE)

assert saved_payments.count() == 103_886

saved_payments.printSchema()
print(f"Successfully wrote {TARGET_TABLE}")