# Databricks notebook source
from pyspark.sql import functions as F

customers = spark.table("workspace.olist_bronze.customers")

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
]

display(
    customers.agg(
        F.count("*").alias("rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("customer_unique_id").alias(
            "distinct_unique_customers"
        ),
        *[
            F.sum(
                F.when(
                    F.col(column).isNull()
                    | (F.trim(F.col(column)) == ""),
                    1,
                ).otherwise(0)
            ).alias(f"{column}_missing")
            for column in CUSTOMER_COLUMNS
        ],
    )
)

# COMMAND ----------

customers_prepared = (
    customers
    .withColumn(
        "customer_zip_code_prefix",
        F.trim(F.col("customer_zip_code_prefix")),
    )
    .withColumn(
        "customer_city",
        F.lower(F.trim(F.col("customer_city"))),
    )
    .withColumn(
        "customer_state",
        F.upper(F.trim(F.col("customer_state"))),
    )
)

# COMMAND ----------

BRAZIL_STATE_CODES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF",
    "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS",
    "RO", "RR", "SC", "SP", "SE", "TO",
]

display(
    customers_prepared.agg(
        F.sum(
            F.when(
                F.col("customer_zip_code_prefix").isNotNull()
                & ~F.col("customer_zip_code_prefix").rlike(r"^\d{5}$"),
                1,
            ).otherwise(0)
        ).alias("invalid_zip_format"),

        F.sum(
            F.when(
                F.col("customer_state").isNotNull()
                & ~F.col("customer_state").isin(BRAZIL_STATE_CODES),
                1,
            ).otherwise(0)
        ).alias("invalid_state_code"),
    )
)

# COMMAND ----------

orphan_orders = (
    spark.table("workspace.olist_silver.orders")
    .join(
        customers_prepared.select("customer_id"),
        on="customer_id",
        how="left_anti",
    )
)

print(f"Orders without a matching customer: {orphan_orders.count():,}")

# COMMAND ----------

customers_silver = customers_prepared

checks = customers_silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("customer_id").alias("distinct_customer_ids"),

    *[
        F.sum(
            F.when(
                F.col(column).isNull()
                | (F.trim(F.col(column)) == ""),
                1,
            ).otherwise(0)
        ).alias(f"{column}_missing")
        for column in CUSTOMER_COLUMNS
    ],

    F.sum(
        F.when(
            ~F.col("customer_zip_code_prefix").rlike(r"^\d{5}$"),
            1,
        ).otherwise(0)
    ).alias("invalid_zip_format"),

    F.sum(
        F.when(
            ~F.col("customer_state").isin(BRAZIL_STATE_CODES),
            1,
        ).otherwise(0)
    ).alias("invalid_state_code"),
).first().asDict()

assert checks["rows"] == 99_441, "Unexpected customer count"

assert checks["distinct_customer_ids"] == checks["rows"], (
    "Duplicate or null customer IDs detected"
)

for column in CUSTOMER_COLUMNS:
    assert checks[f"{column}_missing"] == 0, (
        f"Missing required values: {column}"
    )

assert checks["invalid_zip_format"] == 0, "Invalid ZIP prefixes"
assert checks["invalid_state_code"] == 0, "Invalid state codes"

orphan_count = (
    spark.table("workspace.olist_silver.orders")
    .join(
        customers_silver.select("customer_id"),
        on="customer_id",
        how="left_anti",
    )
    .count()
)

assert orphan_count == 0, (
    f"{orphan_count:,} orders reference missing customers"
)

print("Customer publication checks passed.")

# COMMAND ----------

TARGET_TABLE = "workspace.olist_silver.customers"

(
    customers_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

saved_customers = spark.table(TARGET_TABLE)

saved_checks = saved_customers.agg(
    F.count("*").alias("rows"),
    F.countDistinct("customer_id").alias("distinct_customer_ids"),
    F.countDistinct("customer_unique_id").alias(
        "distinct_unique_customers"
    ),
).first().asDict()

assert saved_checks["rows"] == 99_441
assert saved_checks["distinct_customer_ids"] == saved_checks["rows"]
assert saved_checks["distinct_unique_customers"] == 96_096

saved_customers.printSchema()
print(saved_checks)
print(f"Successfully wrote {TARGET_TABLE}")