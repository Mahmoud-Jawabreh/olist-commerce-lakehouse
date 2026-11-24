import csv
from collections import Counter
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIRECTORY = PROJECT_ROOT / "data" / "raw"

csv_files = sorted(RAW_DIRECTORY.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(f"No CSV files found in {RAW_DIRECTORY}")

for file_path in csv_files:
    missing_counts = Counter()
    row_count = 0

    with file_path.open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as source:
        reader = csv.DictReader(source)
        columns = reader.fieldnames

        if not columns:
            raise ValueError(f"No header found in {file_path.name}")

        for row in reader:
            row_count += 1

            for column in columns:
                value = row.get(column)

                if value is None or not value.strip():
                    missing_counts[column] += 1

    print(f"\n{'=' * 70}")
    print(f"File: {file_path.name}")
    print(f"Rows: {row_count:,}")
    print(f"Columns ({len(columns)}): {', '.join(columns)}")

    print("Missing values:")
    if missing_counts:
        for column in columns:
            count = missing_counts[column]

            if count:
                percentage = count / row_count * 100
                print(f"  {column}: {count:,} ({percentage:.2f}%)")
    else:
        print("  None")