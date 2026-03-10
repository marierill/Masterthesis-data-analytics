from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate generated benchmark tables.")
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Directory containing generated table files for one volume.",
    )
    parser.add_argument(
        "--expected-rows",
        type=int,
        default=None,
        help="Optional expected row count for fact_billing_lines.",
    )
    return parser.parse_args()


def read_table(base_dir: Path, table_name: str) -> pd.DataFrame:
    parquet_path = base_dir / f"{table_name}.parquet"
    csv_path = base_dir / f"{table_name}.csv"

    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)

    raise FileNotFoundError(f"Missing table {table_name} (.parquet or .csv) in {base_dir}")


def validate(data_dir: Path, expected_rows: int | None) -> list[str]:
    errors: list[str] = []

    dim_date = read_table(data_dir, "dim_date")
    dim_customer = read_table(data_dir, "dim_customer")
    dim_product = read_table(data_dir, "dim_product")
    dim_region = read_table(data_dir, "dim_region")
    dim_costcenter = read_table(data_dir, "dim_costcenter")
    fact = read_table(data_dir, "fact_billing_lines")

    if expected_rows is not None and fact.shape[0] != expected_rows:
        errors.append(
            f"Row count mismatch in fact_billing_lines: expected={expected_rows}, actual={fact.shape[0]}"
        )

    fk_checks = [
        ("date_key", set(dim_date["date_key"])),
        ("customer_key", set(dim_customer["customer_key"])),
        ("product_key", set(dim_product["product_key"])),
        ("region_key", set(dim_region["region_key"])),
        ("costcenter_key", set(dim_costcenter["costcenter_key"])),
    ]

    for col, allowed in fk_checks:
        invalid = ~fact[col].isin(allowed)
        invalid_count = int(invalid.sum())
        if invalid_count:
            errors.append(f"Foreign-key integrity violation in {col}: {invalid_count} invalid rows")

    non_negative_cols = ["quantity", "unit_price", "discount_amount", "revenue", "cost"]
    for col in non_negative_cols:
        negative_count = int((fact[col] < 0).sum())
        if negative_count:
            errors.append(f"Negative values in {col}: {negative_count} rows")

    if int((fact["revenue"] == 0).sum()) > 0:
        errors.append("Found zero revenue rows, expected strictly positive billed revenue")

    if int((fact["billing_period_end"] < fact["billing_period_start"]).sum()) > 0:
        errors.append("billing_period_end is earlier than billing_period_start in some rows")

    return errors


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)

    errors = validate(data_dir=data_dir, expected_rows=args.expected_rows)
    if errors:
        print("Validation failed:")
        for err in errors:
            print(f"- {err}")
        raise SystemExit(1)

    print(f"Validation successful for {data_dir}")


if __name__ == "__main__":
    main()
