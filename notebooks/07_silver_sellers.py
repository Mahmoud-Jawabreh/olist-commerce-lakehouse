# Databricks notebook source
from pyspark.sql import functions as F

sellers = spark.table("workspace.olist_bronze.sellers")

SELLER_COLUMNS = [
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
]

sellers_prepared = (
    sellers
    .withColumn(
        "seller_zip_code_prefix",
        F.trim(F.col("seller_zip_code_prefix")),
    )
    .withColumn(
        "seller_city",
        F.lower(F.trim(F.col("seller_city"))),
    )
    .withColumn(
        "seller_state",
        F.upper(F.trim(F.col("seller_state"))),
    )
)

# COMMAND ----------

BRAZIL_STATE_CODES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF",
    "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS",
    "RO", "RR", "SC", "SP", "SE", "TO",
]

checks = sellers_prepared.agg(
    F.count("*").alias("rows"),
    F.countDistinct("seller_id").alias("distinct_seller_ids"),

    *[
        F.sum(
            F.when(
                F.col(column).isNull()
                | (F.trim(F.col(column)) == ""),
                1,
            ).otherwise(0)
        ).alias(f"{column}_missing")
        for column in SELLER_COLUMNS
    ],

    F.sum(
        F.when(
            ~F.col("seller_zip_code_prefix").rlike(r"^[0-9]{5}$"),
            1,
        ).otherwise(0)
    ).alias("invalid_zip_format"),

    F.sum(
        F.when(
            ~F.col("seller_state").isin(BRAZIL_STATE_CODES),
            1,
        ).otherwise(0)
    ).alias("invalid_state_code"),
)

display(checks)

# COMMAND ----------

orphan_items = (
    spark.table("workspace.olist_silver.order_items")
    .join(
        sellers_prepared.select("seller_id"),
        on="seller_id",
        how="left_anti",
    )
)

orphan_count = orphan_items.count()

print(f"Items without a matching seller: {orphan_count:,}")

# COMMAND ----------

sellers_silver = sellers_prepared

check_values = checks.first().asDict()

assert check_values["rows"] == 3_095, "Unexpected seller count"

assert check_values["distinct_seller_ids"] == check_values["rows"], (
    "Duplicate or null seller IDs detected"
)

for column in SELLER_COLUMNS:
    assert check_values[f"{column}_missing"] == 0, (
        f"Missing required values: {column}"
    )

assert check_values["invalid_zip_format"] == 0, "Invalid ZIP prefixes"
assert check_values["invalid_state_code"] == 0, "Invalid state codes"
assert orphan_count == 0, "Items reference missing sellers"

print("Seller publication checks passed.")

# COMMAND ----------

TARGET_TABLE = "workspace.olist_silver.sellers"

(
    sellers_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

saved_sellers = spark.table(TARGET_TABLE)

saved_checks = saved_sellers.agg(
    F.count("*").alias("rows"),
    F.countDistinct("seller_id").alias("distinct_seller_ids"),
).first().asDict()

assert saved_checks["rows"] == 3_095
assert saved_checks["distinct_seller_ids"] == saved_checks["rows"]

saved_sellers.printSchema()
print(saved_checks)
print(f"Successfully wrote {TARGET_TABLE}")