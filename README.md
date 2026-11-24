# Olist Commerce Lakehouse

A batch data engineering project that turns nine Brazilian e-commerce CSVs into analytics tables using PySpark, Delta Lake, Unity Catalog, and Databricks Lakeflow Jobs. The pipeline builds 23 Delta tables, verifies that unchanged inputs reproduce the same business data, and records Gold data-quality metrics by job run.

## Architecture

| Layer | Location | Responsibility |
| --- | --- | --- |
| Raw | `/Volumes/workspace/olist_raw/source_files/` | Original CSV files |
| Bronze | `workspace.olist_bronze` | Nine source-preserving Delta tables with file provenance and ingestion timestamps |
| Silver | `workspace.olist_silver` | Nine typed and validated tables, including a ZIP prefix lookup |
| Gold | `workspace.olist_gold` | Five analytics tables for orders, sales, products, customers, and delivery |
| Audit | `workspace.olist_audit.gold_run_metrics` | Gold row counts and quality flag counts per Lakeflow run |

## Source data

Download the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Extract the eight Olist CSVs for customers, geolocation, order items, payments, reviews, orders, products, and sellers, plus `product_category_name_translation.csv`. Keep the original filenames. Local CSVs belong under the Git-ignored `data/` directory.

## Run the project

1. Use a Databricks workspace with serverless notebook compute and write access to the `workspace` catalog. Import the Python source notebooks from `notebooks/` as Databricks notebooks.
2. Run `00_project_setup` to create the project schemas and raw volume. Upload all nine CSVs to the volume root and verify the listing.
3. Run notebooks `01` through `14` in numeric order. Let each notebook finish its validations before starting the next one.
4. To orchestrate the pipeline, create a Lakeflow Job named `olist_lakehouse_full_refresh`. Use serverless Notebook tasks for `00` through `14`, each depending on the previous task with **All succeeded**. Add `16_audit_gold` after `14` and configure its notebook parameter `job_run_id` as `{{job.run_id}}`.
5. Trigger **Run now** and verify all 16 tasks succeed. In the audit output, confirm five rows for that run ID. The audit merges by job run ID and table name so retrying the audit task does not duplicate its records.

| Notebook range | Work performed |
| --- | --- |
| `00–01` | Create project objects, ingest the CSVs, and validate Bronze row counts |
| `02–05` | Prepare Silver orders, items, payments, and reviews |
| `06–09` | Prepare Silver customers, sellers, products, category translations, and geolocation |
| `10–14` | Build and reconcile the five Gold tables |
| `15_validate_replay` | Compare two pinned Delta snapshots per table; run separately |
| `16_audit_gold` | Verify Gold row counts and persist counts of true quality flags |

The `scripts/` directory contains the local source profiling, candidate-key, and relationship checks used before developing the notebooks.

## Modeling and data quality

- Bronze reads CSV columns as strings and supports quoted multiline fields. Original CSVs remain available for inspection.
- Silver validates required fields, candidate keys, type conversions, numeric ranges, and parent references. The item key is `(order_id, order_item_id)`, the payment key is `(order_id, payment_sequential)`, and the review key is `(review_id, order_id)`.
- Order chronology anomalies and incomplete product attributes remain visible through quality flags. Optional fields stay nullable. ZIP prefixes remain strings to preserve leading zeros.
- Geolocation collapses 1,000,163 source rows to 19,015 ZIP prefixes. It removes exact duplicate observations, screens coordinates with a broad bounding box, and calculates median latitude and longitude separately. The most frequent city/state pair is selected with a deterministic tie-break. Coordinates are approximate. ZIPs without usable coordinates remain in the lookup, and tested left joins preserved all customer and seller rows.
- Gold aggregates items, payments, and reviews separately before joining them to orders. This preserves order grain and prevents monetary totals from being multiplied by child joins.

## Gold tables and measured results

| Table | Grain and metric rules | Verified rows |
| --- | --- | ---: |
| `order_summary` | One row per order across all statuses. Child counts default to zero when absent. Missing amounts and review averages stay null. | 99,441 |
| `daily_sales` | One row per purchase date with delivered orders. Merchandise, freight, and recorded payments are separate measures. | 612 |
| `product_performance` | One row per product appearing in a delivered order. Includes item volume, merchandise value, freight, and category labels where available. | 32,216 |
| `customer_summary` | One row per `customer_unique_id`. Counts all orders and sums sales amounts from delivered orders. Repeat buyers have at least two delivered orders. | 96,096 |
| `delivery_performance` | One row per delivered order. Duration uses calendar days and requires a valid purchase-to-delivery sequence. On-time evaluation also requires an estimate and includes deliveries made on the estimated date. | 96,478 |

- Delivered orders contributed **110,197 item rows**, **13,221,498.11** in merchandise value, **2,198,275.64** in freight, and **15,422,461.77** in recorded payments.
- **2,801 customers** had at least two delivered orders.
- **89,936 of 96,470 eligible deliveries (93.23%)** were on time. Eight delivered orders lacked the actual delivery timestamp and remain in the table.

## Replay verification and operating behavior

A fresh-state run succeeded from setup through Gold. A second run with unchanged source files and notebook code advanced the Delta versions of all 23 pipeline tables. The comparison pinned each before/after version, checked column names and types, then used duplicate-preserving `exceptAll` in both directions. All 23 tables had zero added and zero removed comparison rows after excluding the run-dependent `_ingested_at` column.

To repeat the check, capture the current table versions before refreshing, capture the resulting versions after the refresh, and supply both dictionaries to `15_validate_replay` while both snapshots are available. Each table is replaced atomically. A failure midway through the full refresh can leave a mix of old and new table versions, so inspect the Lakeflow run before relying on refreshed outputs.

After a successful job, inspect the audit records with:

```sql
SELECT job_run_id, table_name, row_count, flag_counts_json, checked_at
FROM workspace.olist_audit.gold_run_metrics
ORDER BY checked_at DESC, table_name;
```
