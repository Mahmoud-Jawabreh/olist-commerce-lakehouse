# Databricks notebook source
from pyspark.sql import functions as F

geo = spark.table("workspace.olist_bronze.geolocation")

geo_prepared = (
    geo
    .withColumn(
        "geolocation_zip_code_prefix",
        F.trim(F.col("geolocation_zip_code_prefix")),
    )
    .withColumn(
        "geolocation_city",
        F.lower(F.trim(F.col("geolocation_city"))),
    )
    .withColumn(
        "geolocation_state",
        F.upper(F.trim(F.col("geolocation_state"))),
    )
    .withColumn(
        "latitude",
        F.expr(
            "try_cast(nullif(trim(geolocation_lat), '') AS DOUBLE)"
        ),
    )
    .withColumn(
        "longitude",
        F.expr(
            "try_cast(nullif(trim(geolocation_lng), '') AS DOUBLE)"
        ),
    )
)

# COMMAND ----------

SOURCE_COLUMNS = [
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state",
]

display(
    geo_prepared.agg(
        F.count("*").alias("rows"),
        F.countDistinct("geolocation_zip_code_prefix").alias(
            "distinct_zip_prefixes"
        ),

        *[
            F.sum(
                F.when(
                    F.col(column).isNull()
                    | (F.trim(F.col(column)) == ""),
                    1,
                ).otherwise(0)
            ).alias(f"{column}_missing")
            for column in SOURCE_COLUMNS
        ],

        F.sum(
            F.when(
                ~F.col("geolocation_zip_code_prefix").rlike(
                    r"^[0-9]{5}$"
                ),
                1,
            ).otherwise(0)
        ).alias("invalid_zip_format"),

        *[
            F.sum(
                F.when(
                    F.col(source).isNotNull()
                    & (F.trim(F.col(source)) != "")
                    & F.col(parsed).isNull(),
                    1,
                ).otherwise(0)
            ).alias(f"{parsed}_parse_failures")
            for source, parsed in [
                ("geolocation_lat", "latitude"),
                ("geolocation_lng", "longitude"),
            ]
        ],

        F.sum(
            F.when(
                F.isnan("latitude")
                | ~F.col("latitude").between(-90, 90),
                1,
            ).otherwise(0)
        ).alias("invalid_latitude"),

        F.sum(
            F.when(
                F.isnan("longitude")
                | ~F.col("longitude").between(-180, 180),
                1,
            ).otherwise(0)
        ).alias("invalid_longitude"),

        F.min("latitude").alias("min_latitude"),
        F.max("latitude").alias("max_latitude"),
        F.min("longitude").alias("min_longitude"),
        F.max("longitude").alias("max_longitude"),
    )
)

# COMMAND ----------

source_row_count = geo.count()
distinct_source_rows = geo.select(*SOURCE_COLUMNS).distinct().count()

print(
    "Extra exact duplicate source rows:",
    f"{source_row_count - distinct_source_rows:,}",
)

zip_profile = (
    geo_prepared
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.count("*").alias("observations"),
        F.countDistinct("geolocation_city").alias("city_names"),
        F.countDistinct("geolocation_state").alias("state_codes"),
    )
)

display(
    zip_profile.agg(
        F.max("observations").alias("max_observations_per_zip"),
        F.sum(
            F.when(F.col("city_names") > 1, 1).otherwise(0)
        ).alias("zip_prefixes_with_multiple_city_names"),
        F.sum(
            F.when(F.col("state_codes") > 1, 1).otherwise(0)
        ).alias("zip_prefixes_with_multiple_states"),
    )
)

display(
    zip_profile
    .filter(F.col("state_codes") > 1)
    .orderBy(F.desc("observations"))
    .limit(10)
)

# COMMAND ----------

geo_screened = geo_prepared.withColumn(
    "_dq_outside_brazil_screen",
    ~(
        F.col("latitude").between(-34, 6)
        & F.col("longitude").between(-74, -28)
    ),
)

display(
    geo_screened.agg(
        F.sum(
            F.col("_dq_outside_brazil_screen").cast("long")
        ).alias("observations_outside_screen"),

        F.countDistinct(
            F.when(
                F.col("_dq_outside_brazil_screen"),
                F.col("geolocation_zip_code_prefix"),
            )
        ).alias("zip_prefixes_with_outside_observations"),
    )
)

# COMMAND ----------

zip_coordinate_coverage = (
    geo_screened
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.count("*").alias("observations"),

        F.sum(
            F.when(
                ~F.col("_dq_outside_brazil_screen"), 1
            ).otherwise(0)
        ).alias("observations_inside_screen"),
    )
)

display(
    zip_coordinate_coverage
    .filter(F.col("observations_inside_screen") == 0)
    .orderBy("geolocation_zip_code_prefix")
)

# COMMAND ----------

exact_duplicate_extras = (
    geo.count()
    - geo.select(*SOURCE_COLUMNS).distinct().count()
)

print(f"Extra exact duplicate source rows: {exact_duplicate_extras:,}")

# COMMAND ----------

from pyspark.sql.window import Window

# Exclude ingestion metadata when identifying exact source duplicates.
geo_unique = (
    geo.select(*SOURCE_COLUMNS)
    .distinct()
    .withColumn(
        "geolocation_zip_code_prefix",
        F.trim(F.col("geolocation_zip_code_prefix")),
    )
    .withColumn(
        "geolocation_city",
        F.lower(F.trim(F.col("geolocation_city"))),
    )
    .withColumn(
        "geolocation_state",
        F.upper(F.trim(F.col("geolocation_state"))),
    )
    .withColumn(
        "latitude",
        F.expr("try_cast(geolocation_lat AS DOUBLE)"),
    )
    .withColumn(
        "longitude",
        F.expr("try_cast(geolocation_lng AS DOUBLE)"),
    )
    .withColumn(
        "_outside_screen",
        ~(
            F.col("latitude").between(-34, 6)
            & F.col("longitude").between(-74, -28)
        ),
    )
)

# COMMAND ----------

raw_counts = (
    geo_prepared
    .groupBy("geolocation_zip_code_prefix")
    .agg(F.count("*").alias("source_row_count"))
)

zip_stats = (
    geo_unique
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.count("*").alias("distinct_observation_count"),

        F.sum(
            F.col("_outside_screen").cast("long")
        ).alias("outside_screen_count"),

        F.countDistinct("geolocation_city").alias("city_name_count"),
        F.countDistinct("geolocation_state").alias("state_code_count"),

        F.expr("""
            percentile(
                CASE WHEN NOT _outside_screen THEN latitude END,
                0.5
            )
        """).alias("latitude"),

        F.expr("""
            percentile(
                CASE WHEN NOT _outside_screen THEN longitude END,
                0.5
            )
        """).alias("longitude"),
    )
)

# COMMAND ----------

label_counts = (
    geo_unique
    .groupBy(
        "geolocation_zip_code_prefix",
        "geolocation_city",
        "geolocation_state",
    )
    .agg(F.count("*").alias("label_observation_count"))
)

label_window = (
    Window
    .partitionBy("geolocation_zip_code_prefix")
    .orderBy(
        F.desc("label_observation_count"),
        F.asc("geolocation_state"),
        F.asc("geolocation_city"),
    )
)

representative_labels = (
    label_counts
    .withColumn("_rank", F.row_number().over(label_window))
    .filter(F.col("_rank") == 1)
    .select(
        "geolocation_zip_code_prefix",
        "geolocation_city",
        "geolocation_state",
        "label_observation_count",
    )
)

# COMMAND ----------

geolocation_silver = (
    zip_stats
    .join(
        raw_counts,
        on="geolocation_zip_code_prefix",
        how="inner",
    )
    .join(
        representative_labels,
        on="geolocation_zip_code_prefix",
        how="inner",
    )
    .withColumn(
        "exact_duplicate_extra_count",
        F.col("source_row_count")
        - F.col("distinct_observation_count"),
    )
    .withColumn(
        "_dq_multiple_city_names",
        F.col("city_name_count") > 1,
    )
    .withColumn(
        "_dq_multiple_states",
        F.col("state_code_count") > 1,
    )
    .withColumn(
        "_dq_has_outside_screen_observations",
        F.col("outside_screen_count") > 0,
    )
    .withColumn(
        "_dq_no_screened_coordinates",
        F.col("latitude").isNull()
        | F.col("longitude").isNull(),
    )
)

# COMMAND ----------

checks = geolocation_silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("geolocation_zip_code_prefix").alias(
        "distinct_zip_prefixes"
    ),
    F.sum("source_row_count").alias("source_rows"),
    F.sum("exact_duplicate_extra_count").alias("duplicate_extras"),
).first().asDict()

assert checks["rows"] == 19_015
assert checks["distinct_zip_prefixes"] == checks["rows"]
assert checks["source_rows"] == 1_000_163
assert checks["duplicate_extras"] == 261_831

FLAGS = [
    "_dq_multiple_city_names",
    "_dq_multiple_states",
    "_dq_has_outside_screen_observations",
    "_dq_no_screened_coordinates",
]

display(
    geolocation_silver.agg(*[
        F.sum(F.col(flag).cast("long")).alias(flag)
        for flag in FLAGS
    ])
)

display(
    geolocation_silver
    .filter(F.col("_dq_no_screened_coordinates"))
    .orderBy("geolocation_zip_code_prefix")
)

print(checks)

# COMMAND ----------

TARGET_TABLE = "workspace.olist_silver.geolocation"

(
    geolocation_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

saved_geo = spark.table(TARGET_TABLE)

saved_checks = saved_geo.agg(
    F.count("*").alias("rows"),
    F.countDistinct("geolocation_zip_code_prefix").alias(
        "distinct_zip_prefixes"
    ),
).first().asDict()

assert saved_checks["rows"] == 19_015
assert saved_checks["distinct_zip_prefixes"] == saved_checks["rows"]

print(f"Successfully wrote {TARGET_TABLE}")

# COMMAND ----------

geo_lookup = saved_geo.select(
    F.col("geolocation_zip_code_prefix").alias("_geo_zip"),
    "latitude",
    "longitude",
    "_dq_no_screened_coordinates",
)

join_results = []

for table_name, zip_column, expected_rows in [
    ("customers", "customer_zip_code_prefix", 99_441),
    ("sellers", "seller_zip_code_prefix", 3_095),
]:
    source = spark.table(f"workspace.olist_silver.{table_name}")

    enriched = source.join(
        geo_lookup,
        source[zip_column] == geo_lookup["_geo_zip"],
        how="left",
    )

    result = enriched.agg(
        F.count("*").alias("joined_rows"),

        F.sum(
            F.when(F.col("_geo_zip").isNull(), 1).otherwise(0)
        ).alias("unmatched_zip_rows"),

        F.sum(
            F.when(
                F.col("_geo_zip").isNotNull()
                & F.col("_dq_no_screened_coordinates"),
                1,
            ).otherwise(0)
        ).alias("matched_without_coordinates"),
    ).first()

    assert result["joined_rows"] == expected_rows, (
        f"{table_name}: join changed the expected row count"
    )

    join_results.append((
        table_name,
        result["joined_rows"],
        result["unmatched_zip_rows"],
        result["matched_without_coordinates"],
    ))

display(
    spark.createDataFrame(
        join_results,
        schema="""
            table_name STRING,
            joined_rows LONG,
            unmatched_zip_rows LONG,
            matched_without_coordinates LONG
        """,
    )
)