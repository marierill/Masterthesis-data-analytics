from __future__ import annotations

# Snowflake credentials are loaded from a .env file.
# Create a .env file in the repo root with:
#
#   SNOWFLAKE_ACCOUNT=<your-account-identifier>
#   SNOWFLAKE_USER=<your-username>
#   SNOWFLAKE_PASSWORD=<your-password>
#   SNOWFLAKE_DATABASE=<your-database>
#   SNOWFLAKE_SCHEMA=<your-schema>
#   SNOWFLAKE_WAREHOUSE=<your-warehouse>
#
# The .env file is gitignored - never commit credentials.

import argparse
import os
import time
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


TABLES = [
    "dim_date",
    "dim_product",
    "dim_customer",
    "dim_region",
    "dim_costcenter",
    "fact_billing_lines",
]

STAGE_NAME = "benchmark_stage"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load generated Parquet data into Snowflake.")
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Directory containing generated Parquet files for one volume.",
    )
    parser.add_argument(
        "--schema-sql",
        default="04_cloud_dwh/schema.sql",
        help="Path to Snowflake schema DDL file.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file with Snowflake credentials.",
    )
    return parser.parse_args()


def get_connection(env_file: str) -> snowflake.connector.SnowflakeConnection:
    load_dotenv(env_file)
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )


def load(data_dir: Path, schema_sql_path: Path, env_file: str) -> None:
    print("Connecting to Snowflake...")
    con = get_connection(env_file)
    cur = con.cursor()

    print(f"Applying schema from: {schema_sql_path}")
    for statement in schema_sql_path.read_text(encoding="utf-8").split(";"):
        stmt = statement.strip()
        if stmt and not stmt.startswith("--"):
            cur.execute(stmt)

    print(f"Creating internal stage: {STAGE_NAME}")
    cur.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME} FILE_FORMAT = (TYPE = PARQUET)")

    for table in TABLES:
        parquet_file = data_dir / f"{table}.parquet"
        if not parquet_file.exists():
            raise FileNotFoundError(f"Missing parquet file: {parquet_file}")

        print(f"\n  Uploading {parquet_file.name} to stage...")
        t0 = time.perf_counter()
        cur.execute(f"PUT file://{parquet_file.as_posix()} @{STAGE_NAME}/{table}/ AUTO_COMPRESS=FALSE")

        print(f"  Loading {table} from stage...")
        cur.execute(f"""
            COPY INTO {table}
            FROM @{STAGE_NAME}/{table}/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = FALSE
        """)

        elapsed = time.perf_counter() - t0
        row_count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {row_count:,} rows loaded in {elapsed:.2f}s")

    cur.close()
    con.close()
    print("\nDone.")


def main() -> None:
    args = parse_args()
    load(
        data_dir=Path(args.data_dir),
        schema_sql_path=Path(args.schema_sql),
        env_file=args.env_file,
    )


if __name__ == "__main__":
    main()
