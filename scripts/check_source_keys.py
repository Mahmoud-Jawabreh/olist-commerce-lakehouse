import csv
from collections import Counter
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIRECTORY = PROJECT_ROOT / "data" / "raw"

KEY_CHECKS = {
    "olist_customers_dataset.csv": [
        ("customer_id",),
        ("customer_unique_id",),
    ],
    "olist_orders_dataset.csv": [
        ("order_id",),
    ],
    "olist_order_items_dataset.csv": [
        ("order_id", "order_item_id"),
    ],
    "olist_order_payments_dataset.csv": [
        ("order_id", "payment_sequential"),
    ],
    "olist_order_reviews_dataset.csv": [
        ("review_id",),
        ("order_id",),
        ("review_id", "order_id"),
    ],
    "olist_products_dataset.csv": [
        ("product_id",),
    ],
    "olist_sellers_dataset.csv": [
        ("seller_id",),
    ],
    "product_category_name_translation.csv": [
        ("product_category_name",),
    ],
    "olist_geolocation_dataset.csv": [
        ("geolocation_zip_code_prefix",),
    ],
}

for filename, candidate_keys in KEY_CHECKS.items():
    key_counts = {columns: Counter() for columns in candidate_keys}
    missing_counts = Counter()
    row_count = 0

    with (RAW_DIRECTORY / filename).open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as source:
        reader = csv.DictReader(source)

        required_columns = {
            column
            for columns in candidate_keys
            for column in columns
        }
        absent_columns = required_columns - set(reader.fieldnames or [])

        if absent_columns:
            raise ValueError(
                f"{filename}: missing columns {sorted(absent_columns)}"
            )

        for row in reader:
            row_count += 1

            for columns in candidate_keys:
                values = tuple(row[column] for column in columns)

                if any(
                    value is None or not value.strip()
                    for value in values
                ):
                    missing_counts[columns] += 1
                else:
                    key_counts[columns][values] += 1

    print(f"\nFile: {filename}")
    print(f"Rows: {row_count:,}")

    for columns in candidate_keys:
        counts = key_counts[columns]

        repeated_keys = sum(count > 1 for count in counts.values())
        extra_rows = sum(count - 1 for count in counts.values())

        print(f"  Key: {' + '.join(columns)}")
        print(f"    Rows with missing key fields: {missing_counts[columns]:,}")
        print(f"    Distinct nonmissing keys: {len(counts):,}")
        print(f"    Keys appearing more than once: {repeated_keys:,}")
        print(f"    Extra rows beyond first occurrence: {extra_rows:,}")