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
#
# Note on Parquet date handling:
#   Pandas writes DATE columns as int64 nanosecond timestamps in Parquet.
#   Snowflake COPY INTO cannot cast these directly to DATE.
#   Tables with DATE columns (dim_date, fact_billing_lines) are loaded
#   via a two-step process: first into a VARIANT staging table, then
#   inserted with explicit ::DATE casts into the target table.
#   This is a documented platform difference vs. DuckDB (see thesis ch. 4.3.1).

import argparse
import os
import re
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

# Tables with DATE columns that require variant staging workaround
TABLES_WITH_DATES = {"dim_date", "fact_billing_lines"}

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


def parse_sql_statements(sql_text: str) -> list[str]:
    """Robust SQL parser: strips comments before splitting on semicolons."""
    sql_no_block = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)
    sql_no_comments = re.sub(r'--[^\n]*', '', sql_no_block)
    return [s.strip() for s in sql_no_comments.split(';') if s.strip()]


def upload_to_stage(cur, parquet_file: Path, table: str) -> None:
    """Upload a Parquet file to the internal stage."""
    cur.execute(
        f"PUT file://{parquet_file.as_posix()} @{STAGE_NAME}/{table}/ "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )


def load_standard(cur, table: str) -> int:
    """Load tables without DATE columns using standard COPY INTO."""
    cur.execute(f"""
        COPY INTO {table}
        FROM @{STAGE_NAME}/{table}/
        FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = FALSE
    """)
    return cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def load_dim_date(cur) -> int:
    """Load dim_date via variant staging to handle Pandas date format."""
    cur.execute("CREATE OR REPLACE TEMPORARY TABLE dim_date_stage (v VARIANT)")
    cur.execute(f"""
        COPY INTO dim_date_stage
        FROM @{STAGE_NAME}/dim_date/
        FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)
        PURGE = FALSE
    """)
    cur.execute("""
        INSERT INTO dim_date
        SELECT
            v:date_key::INTEGER,
            v:full_date::DATE,
            v:year::SMALLINT,
            v:month::TINYINT,
            v:quarter::TINYINT,
            v:week::TINYINT,
            v:day_of_week::TINYINT
        FROM dim_date_stage
    """)
    cur.execute("DROP TABLE IF EXISTS dim_date_stage")
    return cur.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]


def load_fact_billing_lines(cur) -> int:
    """Load fact_billing_lines via variant staging to handle Pandas date format."""
    cur.execute("CREATE OR REPLACE TEMPORARY TABLE fact_stage (v VARIANT)")
    cur.execute(f"""
        COPY INTO fact_stage
        FROM @{STAGE_NAME}/fact_billing_lines/
        FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)
        PURGE = FALSE
    """)
    cur.execute("""
        INSERT INTO fact_billing_lines
        SELECT
            v:billing_line_id::NUMBER(18,0),
            v:invoice_id::NUMBER(18,0),
            v:date_key::INTEGER,
            v:customer_key::INTEGER,
            v:product_key::INTEGER,
            v:region_key::INTEGER,
            v:costcenter_key::INTEGER,
            v:subscription_type::VARCHAR,
            v:billing_period_start::DATE,
            v:billing_period_end::DATE,
            v:quantity::SMALLINT,
            v:unit_price::NUMBER(10,2),
            v:discount_amount::NUMBER(10,2),
            v:revenue::NUMBER(10,2),
            v:cost::NUMBER(10,2)
        FROM fact_stage
    """)
    cur.execute("DROP TABLE IF EXISTS fact_stage")
    return cur.execute("SELECT COUNT(*) FROM fact_billing_lines").fetchone()[0]


def load(data_dir: Path, schema_sql_path: Path, env_file: str) -> None:
    print("Connecting to Snowflake...")
    con = get_connection(env_file)
    cur = con.cursor()

    print(f"Applying schema from: {schema_sql_path}")
    statements = parse_sql_statements(schema_sql_path.read_text(encoding="utf-8"))
    print(f"  Executing {len(statements)} SQL statements...")
    for stmt in statements:
        cur.execute(stmt)
    print("  Schema applied successfully.")

    print(f"Creating internal stage: {STAGE_NAME}")
    cur.execute(
        f"CREATE STAGE IF NOT EXISTS {STAGE_NAME} "
        f"FILE_FORMAT = (TYPE = PARQUET)"
    )

    for table in TABLES:
        parquet_file = data_dir / f"{table}.parquet"
        if not parquet_file.exists():
            raise FileNotFoundError(f"Missing parquet file: {parquet_file}")

        t0 = time.perf_counter()
        print(f"\n  Uploading {parquet_file.name} to stage...")
        upload_to_stage(cur, parquet_file, table)

        print(f"  Loading {table}...")
        if table == "dim_date":
            row_count = load_dim_date(cur)
        elif table == "fact_billing_lines":
            row_count = load_fact_billing_lines(cur)
        else:
            row_count = load_standard(cur, table)

        elapsed = time.perf_counter() - t0
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
