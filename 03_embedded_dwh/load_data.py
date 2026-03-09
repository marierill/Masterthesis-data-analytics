from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb


TABLES = [
    "dim_date",
    "dim_product",
    "dim_customer",
    "dim_region",
    "dim_costcenter",
    "fact_billing_lines",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load generated Parquet data into DuckDB.")
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Directory containing generated Parquet files for one volume (e.g. data/generated/small).",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file. Defaults to <data-dir>/benchmark.duckdb.",
    )
    parser.add_argument(
        "--schema",
        default="03_embedded_dwh/schema.sql",
        help="Path to schema DDL file.",
    )
    return parser.parse_args()


def load(data_dir: Path, db_path: Path, schema_path: Path) -> None:
    print(f"Connecting to DuckDB: {db_path}")
    con = duckdb.connect(str(db_path))

    print(f"Applying schema from: {schema_path}")
    con.execute(schema_path.read_text(encoding="utf-8"))

    for table in TABLES:
        parquet_file = data_dir / f"{table}.parquet"
        if not parquet_file.exists():
            raise FileNotFoundError(f"Missing parquet file: {parquet_file}")

        t0 = time.perf_counter()
        con.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{parquet_file.as_posix()}')")
        elapsed = time.perf_counter() - t0

        row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {row_count:,} rows loaded in {elapsed:.2f}s")

    con.close()
    print(f"\nDone. Database: {db_path}")


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path) if args.db_path else data_dir / "benchmark.duckdb"
    schema_path = Path(args.schema)

    load(data_dir=data_dir, db_path=db_path, schema_path=schema_path)


if __name__ == "__main__":
    main()
