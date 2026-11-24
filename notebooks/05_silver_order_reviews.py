# Databricks notebook source
from pyspark.sql import functions as F

reviews = spark.table("workspace.olist_bronze.order_reviews")

REVIEW_COLUMNS = [
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
]

display(
    reviews.agg(
        F.count("*").alias("rows"),
        *[
            F.sum(
                F.when(
                    F.col(column).isNull()
                    | (F.trim(F.col(column)) == ""),
                    1,
                ).otherwise(0)
            ).alias(f"{column}_missing")
            for column in REVIEW_COLUMNS
        ],
    )
)

# COMMAND ----------

display(
    reviews.agg(
        F.count("*").alias("rows"),
        F.countDistinct("review_id").alias("distinct_review_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.countDistinct(
            "review_id", "order_id"
        ).alias("distinct_review_order_pairs"),
    )
)

duplicate_pairs = (
    reviews.groupBy("review_id", "order_id")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_pairs.orderBy(F.desc("count")).limit(20))

# Compare source fields only, excluding ingestion metadata.
source_columns = [
    column for column in reviews.columns
    if not column.startswith("_")
]

exact_duplicate_extras = (
    reviews.count()
    - reviews.select(*source_columns).distinct().count()
)

print(f"Extra exact duplicate source rows: {exact_duplicate_extras:,}")

# COMMAND ----------

TARGET_TYPES = {
    "review_score": "INT",
    "review_creation_date": "TIMESTAMP_NTZ",
    "review_answer_timestamp": "TIMESTAMP_NTZ",
}

reviews_checked = reviews

for column, target_type in TARGET_TYPES.items():
    reviews_checked = reviews_checked.withColumn(
        f"{column}_parsed",
        F.expr(
            f"try_cast(nullif(trim(`{column}`), '') AS {target_type})"
        ),
    )

display(
    reviews_checked.agg(*[
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
)

display(
    reviews_checked
    .groupBy("review_score_parsed")
    .count()
    .orderBy("review_score_parsed")
)

# COMMAND ----------

display(
    reviews_checked.agg(
        F.sum(
            F.when(
                F.col("review_answer_timestamp_parsed")
                < F.col("review_creation_date_parsed"),
                1,
            ).otherwise(0)
        ).alias("answer_before_creation")
    )
)

orphan_count = reviews.join(
    spark.table("workspace.olist_silver.orders").select("order_id"),
    on="order_id",
    how="left_anti",
).count()

print(f"Reviews without a matching Silver order: {orphan_count:,}")

# COMMAND ----------

reviews_silver = reviews_checked.select(*[
    (
        F.col(f"{column}_parsed").alias(column)
        if column in TARGET_TYPES
        else F.col(column)
    )
    for column in reviews.columns
])

# Normalize blank comments to null; preserve nonblank text as supplied.
for column in ["review_comment_title", "review_comment_message"]:
    reviews_silver = reviews_silver.withColumn(
        column,
        F.when(
            F.trim(F.col(column)) == "",
            F.lit(None).cast("string"),
        ).otherwise(F.col(column)),
    )

reviews_silver = reviews_silver.withColumn(
    "_dq_answer_before_creation",
    F.coalesce(
        F.col("review_answer_timestamp")
        < F.col("review_creation_date"),
        F.lit(False),
    ),
)

# COMMAND ----------

REQUIRED_COLUMNS = [
    "review_id",
    "order_id",
    "review_score",
    "review_creation_date",
    "review_answer_timestamp",
]

checks = reviews_silver.agg(
    F.count("*").alias("rows"),

    F.countDistinct(
        "review_id", "order_id"
    ).alias("distinct_review_order_pairs"),

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
        F.when(
            ~F.col("review_score").between(1, 5),
            1,
        ).otherwise(0)
    ).alias("invalid_scores"),
).first().asDict()

assert checks["rows"] == 99_224, "Unexpected review count"

assert checks["distinct_review_order_pairs"] == checks["rows"], (
    "Duplicate review–order pairs detected"
)

for column in REQUIRED_COLUMNS:
    assert checks[f"{column}_missing"] == 0, (
        f"Missing or unparseable values: {column}"
    )

assert checks["invalid_scores"] == 0, "Scores outside 1–5 detected"

orphan_count = reviews_silver.join(
    spark.table("workspace.olist_silver.orders").select("order_id"),
    on="order_id",
    how="left_anti",
).count()

assert orphan_count == 0, (
    f"{orphan_count:,} reviews reference missing Silver orders"
)

print("All review publication checks passed.")

# COMMAND ----------

TARGET_TABLE = "workspace.olist_silver.order_reviews"

(
    reviews_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

saved_reviews = spark.table(TARGET_TABLE)

saved_checks = saved_reviews.agg(
    F.count("*").alias("rows"),
    F.countDistinct(
        "review_id", "order_id"
    ).alias("distinct_review_order_pairs"),
    F.sum(
        F.col("_dq_answer_before_creation").cast("long")
    ).alias("answer_before_creation"),
).first().asDict()

assert saved_checks["rows"] == 99_224
assert saved_checks["distinct_review_order_pairs"] == saved_checks["rows"]

saved_reviews.printSchema()
print(saved_checks)
print(f"Successfully wrote {TARGET_TABLE}")