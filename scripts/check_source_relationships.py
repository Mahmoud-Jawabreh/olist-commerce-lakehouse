import csv
from collections import Counter
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIRECTORY = PROJECT_ROOT / "data" / "raw"

# Each relationship is:
# (child file, child column, parent file, parent column)
RELATIONSHIPS = [
    (
        "olist_orders_dataset.csv", "customer_id",
        "olist_customers_dataset.csv", "customer_id",
    ),
    (
        "olist_order_items_dataset.csv", "order_id",
        "olist_orders_dataset.csv", "order_id",
    ),
    (
        "olist_order_payments_dataset.csv", "order_id",
        "olist_orders_dataset.csv", "order_id",
    ),
    (
        "olist_order_reviews_dataset.csv", "order_id",
        "olist_orders_dataset.csv", "order_id",
    ),
    (
        "olist_order_items_dataset.csv", "product_id",
        "olist_products_dataset.csv", "product_id",
    ),
    (
        "olist_order_items_dataset.csv", "seller_id",
        "olist_sellers_dataset.csv", "seller_id",
    ),
    (
        "olist_products_dataset.csv", "product_category_name",
        "product_category_name_translation.csv", "product_category_name",
    ),
    (
        "olist_customers_dataset.csv", "customer_zip_code_prefix",
        "olist_geolocation_dataset.csv", "geolocation_zip_code_prefix",
    ),
    (
        "olist_sellers_dataset.csv", "seller_zip_code_prefix",
        "olist_geolocation_dataset.csv", "geolocation_zip_code_prefix",
    ),
]

# Cache counts so shared file/column combinations are read only once.
CACHE = {}


def get_counts(filename, column):
    cache_key = (filename, column)

    if cache_key in CACHE:
        return CACHE[cache_key]

    counts = Counter()
    missing_rows = 0

    with (RAW_DIRECTORY / filename).open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as source:
        reader = csv.DictReader(source)

        if column not in (reader.fieldnames or []):
            raise ValueError(f"{filename}: missing column {column}")

        for row in reader:
            value = row[column]

            if value is None or not value.strip():
                missing_rows += 1
            else:
                counts[value] += 1

    CACHE[cache_key] = (counts, missing_rows)
    return counts, missing_rows


for child_file, child_column, parent_file, parent_column in RELATIONSHIPS:
    child_counts, child_missing = get_counts(child_file, child_column)
    parent_counts, parent_missing = get_counts(parent_file, parent_column)

    child_keys = set(child_counts)
    parent_keys = set(parent_counts)

    matched_keys = child_keys & parent_keys
    orphan_keys = child_keys - parent_keys

    orphan_rows = sum(child_counts[key] for key in orphan_keys)
    matched_child_rows = sum(child_counts[key] for key in matched_keys)

    # Count the rows an equality INNER JOIN would produce.
    joined_rows = sum(
        child_counts[key] * parent_counts[key]
        for key in matched_keys
    )

    duplicate_parent_keys = sum(
        count > 1 for count in parent_counts.values()
    )
    referenced_duplicate_keys = sum(
        parent_counts[key] > 1 for key in matched_keys
    )
    parents_without_children = len(parent_keys - child_keys)

    max_children = max(
        (child_counts[key] for key in matched_keys),
        default=0,
    )

    print(f"\nRelationship: {child_file}.{child_column}")
    print(f"           -> {parent_file}.{parent_column}")
    print(f"  Child rows with missing reference: {child_missing:,}")
    print(f"  Parent rows with missing key: {parent_missing:,}")
    print(f"  Distinct orphan references: {len(orphan_keys):,}")
    print(f"  Child rows with orphan references: {orphan_rows:,}")
    print(f"  Duplicate parent keys: {duplicate_parent_keys:,}")
    print(f"  Referenced duplicate parent keys: {referenced_duplicate_keys:,}")
    print(f"  Parent keys without child rows: {parents_without_children:,}")
    print(f"  Maximum child rows per matched key: {max_children:,}")
    print(f"  Matched child rows before join: {matched_child_rows:,}")
    print(f"  Rows after inner join: {joined_rows:,}")
    print(f"  Extra rows caused by parent duplicates: "
          f"{joined_rows - matched_child_rows:,}")

    if orphan_keys:
        print(f"  Example orphan references: {sorted(orphan_keys)[:5]}")


# Check multiplication when joining several order-level child tables.
orders, _ = get_counts("olist_orders_dataset.csv", "order_id")
items, _ = get_counts("olist_order_items_dataset.csv", "order_id")
payments, _ = get_counts("olist_order_payments_dataset.csv", "order_id")
reviews, _ = get_counts("olist_order_reviews_dataset.csv", "order_id")

print("\nOrder-level join fan-out")

for label, other_counts in [
    ("items + payments", payments),
    ("items + reviews", reviews),
]:
    shared_orders = set(orders) & set(items) & set(other_counts)

    item_rows = sum(items[key] for key in shared_orders)
    joined_rows = sum(
        items[key] * other_counts[key]
        for key in shared_orders
    )
    affected_orders = sum(
        other_counts[key] > 1 for key in shared_orders
    )

    print(f"\n  Join: {label} on order_id")
    print(f"    Orders present in both child tables: {len(shared_orders):,}")
    print(f"    Orders whose item rows multiply: {affected_orders:,}")
    print(f"    Item rows before join: {item_rows:,}")
    print(f"    Rows after inner join: {joined_rows:,}")
    print(f"    Extra rows: {joined_rows - item_rows:,}")