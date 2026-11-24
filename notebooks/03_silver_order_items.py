# Databricks notebook source
from pyspark.sql import functions as F

items = spark.table("workspace.olist_bronze.order_items")

items.printSchema()
display(items.limit(5))

# COMMAND ----------

required_columns = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]

display(
    items.agg(
        F.count("*").alias("rows"),
        F.countDistinct(
            "order_id", "order_item_id"
        ).alias("distinct_item_keys"),
        *[
            F.sum(
                F.when(
                    F.col(column).isNull()
                    | (F.trim(F.col(column)) == ""),
                    1,
                ).otherwise(0)
            ).alias(f"{column}_missing")
            for column in required_columns
        ],
    )
)

# COMMAND ----------

TARGET_TYPES = {
    "order_item_id": "INT",
    "shipping_limit_date": "TIMESTAMP_NTZ",
    "price": "DECIMAL(18,2)",
    "freight_value": "DECIMAL(18,2)",
}

items_checked = items

for column, target_type in TARGET_TYPES.items():
    items_checked = items_checked.withColumn(
        f"{column}_parsed",
        F.expr(
            f"try_cast(nullif(trim(`{column}`), '') AS {target_type})"
        ),
    )

# COMMAND ----------

parse_checks = items_checked.agg(*[
    F.sum(
        F.when(
            F.col(column).isNotNull()
            & (F.trim(F.col(column)) != "")
            & F.col(f"{column}_parsed").isNull(),
            1,
        ).otherwise(0)
    ).alias(f"{column}_parse_failures")
    for column in TARGET_TYPES
])

display(parse_checks)

failures = parse_checks.first().asDict()

assert all(count == 0 for count in failures.values()), (
    f"Conversion failures: {failures}"
)

# COMMAND ----------

display(
    items_checked.agg(*[
        F.sum(
            F.when(
                ~F.trim(F.col(column)).rlike(
                    r"^[+-]?\d+(\.\d{1,2})?$"
                ),
                1,
            ).otherwise(0)
        ).alias(f"{column}_unexpected_format")
        for column in ["price", "freight_value"]
    ])
)

# COMMAND ----------

display(
    items_checked.agg(
        F.min("order_item_id_parsed").alias("min_item_id"),
        F.max("order_item_id_parsed").alias("max_item_id"),

        F.min("price_parsed").alias("min_price"),
        F.max("price_parsed").alias("max_price"),

        F.min("freight_value_parsed").alias("min_freight"),
        F.max("freight_value_parsed").alias("max_freight"),

        F.sum(
            F.when(F.col("order_item_id_parsed") <= 0, 1).otherwise(0)
        ).alias("nonpositive_item_ids"),

        F.sum(
            F.when(F.col("price_parsed") < 0, 1).otherwise(0)
        ).alias("negative_prices"),

        F.sum(
            F.when(F.col("price_parsed") == 0, 1).otherwise(0)
        ).alias("zero_prices"),

        F.sum(
            F.when(F.col("freight_value_parsed") < 0, 1).otherwise(0)
        ).alias("negative_freight"),

        F.sum(
            F.when(F.col("freight_value_parsed") == 0, 1).otherwise(0)
        ).alias("zero_freight"),
    )
)

# COMMAND ----------

silver_order_ids = (
    spark.table("workspace.olist_silver.orders")
    .select("order_id")
)

orphan_items = items_checked.join(
    silver_order_ids,
    on="order_id",
    how="left_anti",
)

orphan_count = orphan_items.count()

print(f"Items without a matching Silver order: {orphan_count:,}")

assert orphan_count == 0, "Items reference missing Silver orders"

# COMMAND ----------

items_silver = items_checked.select(*[
    (
        F.col(f"{column}_parsed").alias(column)
        if column in TARGET_TYPES
        else F.col(column)
    )
    for column in items.columns
])

# COMMAND ----------

REQUIRED_COLUMNS = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]

checks = items_silver.agg(
    F.count("*").alias("rows"),

    F.countDistinct(
        "order_id", "order_item_id"
    ).alias("distinct_item_keys"),

    *[
        F.sum(
            F.when(
                F.col(column).isNull()
                | (F.trim(F.col(column).cast("string")) == ""),
                1,
            ).otherwise(0)
        ).alias(f"{column}_missing")
        for column in REQUIRED_COLUMNS
    ],

    F.sum(
        F.when(F.col("order_item_id") <= 0, 1).otherwise(0)
    ).alias("nonpositive_item_ids"),

    F.sum(
        F.when(F.col("price") <= 0, 1).otherwise(0)
    ).alias("nonpositive_prices"),

    F.sum(
        F.when(F.col("freight_value") < 0, 1).otherwise(0)
    ).alias("negative_freight"),
).first().asDict()

assert checks["rows"] == 112_650
assert checks["distinct_item_keys"] == checks["rows"]

for column in REQUIRED_COLUMNS:
    assert checks[f"{column}_missing"] == 0, (
        f"Missing required values: {column}"
    )

for rule in [
    "nonpositive_item_ids",
    "nonpositive_prices",
    "negative_freight",
]:
    assert checks[rule] == 0, f"Quality check failed: {rule}"

assert orphan_count == 0, "Items reference missing Silver orders"

print("All item publication checks passed.")

# COMMAND ----------

TARGET_TABLE = "workspace.olist_silver.order_items"

(
    items_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

saved_items = spark.table(TARGET_TABLE)

assert saved_items.count() == 112_650

saved_items.printSchema()
print(f"Successfully wrote {TARGET_TABLE}")