# Databricks notebook source
from pyspark.sql import functions as F

products = spark.table("workspace.olist_bronze.products")
translations = spark.table(
    "workspace.olist_bronze.category_translation"
)

products.printSchema()
translations.printSchema()

# COMMAND ----------

PRODUCT_COLUMNS = [
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]

display(
    products.agg(
        F.count("*").alias("rows"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        *[
            F.sum(
                F.when(
                    F.col(column).isNull()
                    | (F.trim(F.col(column)) == ""),
                    1,
                ).otherwise(0)
            ).alias(f"{column}_missing")
            for column in PRODUCT_COLUMNS
        ],
    )
)

# COMMAND ----------

NUMERIC_COLUMNS = [
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]

products_checked = products

for column in NUMERIC_COLUMNS:
    products_checked = products_checked.withColumn(
        f"{column}_parsed",
        F.expr(
            f"try_cast(nullif(trim(`{column}`), '') AS INT)"
        ),
    )

numeric_profile = []

for column in NUMERIC_COLUMNS:
    result = products_checked.agg(
        F.sum(
            F.when(
                F.col(column).isNotNull()
                & (F.trim(F.col(column)) != "")
                & F.col(f"{column}_parsed").isNull(),
                1,
            ).otherwise(0)
        ).alias("parse_failures"),

        F.min(f"{column}_parsed").alias("minimum"),
        F.max(f"{column}_parsed").alias("maximum"),

        F.sum(
            F.when(
                F.col(f"{column}_parsed") < 0, 1
            ).otherwise(0)
        ).alias("negative_values"),

        F.sum(
            F.when(
                F.col(f"{column}_parsed") == 0, 1
            ).otherwise(0)
        ).alias("zero_values"),
    ).first()

    numeric_profile.append((column, *tuple(result)))

display(
    spark.createDataFrame(
        numeric_profile,
        schema="""
            column_name STRING,
            parse_failures LONG,
            minimum INT,
            maximum INT,
            negative_values LONG,
            zero_values LONG
        """,
    )
)

# COMMAND ----------

def normalize_category(column):
    trimmed = F.trim(F.col(column))

    return F.when(
        trimmed == "",
        F.lit(None).cast("string"),
    ).otherwise(F.lower(trimmed))


products_prepared = products_checked.withColumn(
    "product_category_name",
    normalize_category("product_category_name"),
)

translations_prepared = (
    translations
    .withColumn(
        "product_category_name",
        normalize_category("product_category_name"),
    )
    .withColumn(
        "product_category_name_english",
        normalize_category("product_category_name_english"),
    )
)

display(
    translations_prepared.agg(
        F.count("*").alias("rows"),
        F.countDistinct("product_category_name").alias(
            "distinct_categories"
        ),
        *[
            F.sum(
                F.when(F.col(column).isNull(), 1).otherwise(0)
            ).alias(f"{column}_missing")
            for column in [
                "product_category_name",
                "product_category_name_english",
            ]
        ],
    )
)

# COMMAND ----------

untranslated_products = (
    products_prepared
    .filter(F.col("product_category_name").isNotNull())
    .join(
        translations_prepared.select("product_category_name"),
        on="product_category_name",
        how="left_anti",
    )
)

display(
    untranslated_products
    .groupBy("product_category_name")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

orphan_item_count = (
    spark.table("workspace.olist_silver.order_items")
    .join(
        products_prepared.select("product_id"),
        on="product_id",
        how="left_anti",
    )
    .count()
)

print(f"Items without a matching product: {orphan_item_count:,}")

# COMMAND ----------

translations_silver = translations_prepared

translation_checks = translations_silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("product_category_name").alias("distinct_categories"),
    *[
        F.sum(
            F.when(F.col(column).isNull(), 1).otherwise(0)
        ).alias(f"{column}_missing")
        for column in [
            "product_category_name",
            "product_category_name_english",
        ]
    ],
).first().asDict()

assert translation_checks["rows"] == 71
assert translation_checks["distinct_categories"] == 71
assert translation_checks["product_category_name_missing"] == 0
assert translation_checks["product_category_name_english_missing"] == 0

(
    translations_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.olist_silver.category_translation")
)

# COMMAND ----------

RENAMES = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
}

products_silver = products_prepared.select(*[
    (
        F.col(f"{column}_parsed").alias(RENAMES.get(column, column))
        if column in NUMERIC_COLUMNS
        else F.col(column)
    )
    for column in products.columns
])

# COMMAND ----------

category_lookup = (
    spark.table("workspace.olist_silver.category_translation")
    .select("product_category_name")
    .withColumn("_translation_exists", F.lit(True))
)

products_silver = (
    products_silver
    .join(category_lookup, on="product_category_name", how="left")
    .withColumn(
        "_dq_missing_category",
        F.col("product_category_name").isNull(),
    )
    .withColumn(
        "_dq_untranslated_category",
        F.col("product_category_name").isNotNull()
        & F.col("_translation_exists").isNull(),
    )
    .withColumn(
        "_dq_missing_descriptive_attributes",
        F.col("product_name_length").isNull()
        | F.col("product_description_length").isNull()
        | F.col("product_photos_qty").isNull(),
    )
    .withColumn(
        "_dq_missing_physical_attributes",
        F.col("product_weight_g").isNull()
        | F.col("product_length_cm").isNull()
        | F.col("product_height_cm").isNull()
        | F.col("product_width_cm").isNull(),
    )
    .withColumn(
        "_dq_zero_weight",
        F.coalesce(F.col("product_weight_g") == 0, F.lit(False)),
    )
    .drop("_translation_exists")
)

# COMMAND ----------

parse_checks = products_checked.agg(*[
    F.sum(
        F.when(
            F.col(column).isNotNull()
            & (F.trim(F.col(column)) != "")
            & F.col(f"{column}_parsed").isNull(),
            1,
        ).otherwise(0)
    ).alias(column)
    for column in NUMERIC_COLUMNS
]).first().asDict()

assert all(value == 0 for value in parse_checks.values()), (
    f"Numeric conversion failures: {parse_checks}"
)

# COMMAND ----------

SILVER_NUMERIC_COLUMNS = [
    RENAMES.get(column, column)
    for column in NUMERIC_COLUMNS
]

checks = products_silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("product_id").alias("distinct_product_ids"),

    F.sum(
        F.when(
            F.col("product_id").isNull()
            | (F.trim(F.col("product_id")) == ""),
            1,
        ).otherwise(0)
    ).alias("missing_product_ids"),

    *[
        F.sum(
            F.when(F.col(column) < 0, 1).otherwise(0)
        ).alias(f"{column}_negative")
        for column in SILVER_NUMERIC_COLUMNS
    ],
).first().asDict()

assert checks["rows"] == 32_951
assert checks["distinct_product_ids"] == checks["rows"]
assert checks["missing_product_ids"] == 0

for column in SILVER_NUMERIC_COLUMNS:
    assert checks[f"{column}_negative"] == 0, (
        f"Negative values detected: {column}"
    )

orphan_item_count = (
    spark.table("workspace.olist_silver.order_items")
    .join(
        products_silver.select("product_id"),
        on="product_id",
        how="left_anti",
    )
    .count()
)

assert orphan_item_count == 0, "Items reference missing products"

# COMMAND ----------

TARGET_TABLE = "workspace.olist_silver.products"

(
    products_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

saved_products = spark.table(TARGET_TABLE)

assert saved_products.count() == 32_951

FLAGS = [
    "_dq_missing_category",
    "_dq_untranslated_category",
    "_dq_missing_descriptive_attributes",
    "_dq_missing_physical_attributes",
    "_dq_zero_weight",
]

display(
    saved_products.agg(*[
        F.sum(F.col(flag).cast("long")).alias(flag)
        for flag in FLAGS
    ])
)

print("Successfully wrote Silver products and category translations.")