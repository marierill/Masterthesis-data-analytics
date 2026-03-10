from __future__ import annotations

# Snowflake credentials: see 04_cloud_dwh/load_data.py for .env setup instructions.
# Result caching MUST be disabled before benchmarking:
#   ALTER SESSION SET USE_CACHED_RESULT = FALSE;
# This is done automatically in this script.

import argparse
import csv
import os
import re
import statistics
import time
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


N_RUNS = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Snowflake KPI benchmark.")
    parser.add_argument(
        "--queries",
        default="04_cloud_dwh/kpi_queries.sql",
        help="Path to KPI SQL file.",
    )
    parser.add_argument(
        "--volume",
        required=True,
        choices=["small", "medium", "large"],
        help="Volume label (for result metadata).",
    )
    parser.add_argument(
        "--output-dir",
        default="05_benchmark_results",
        help="Directory for result CSV output.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=N_RUNS,
        help=f"Number of repetitions per query (default: {N_RUNS}).",
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


def parse_queries(sql_text: str) -> list[tuple[str, str]]:
    """Splits SQL file into (label, sql) pairs based on Q-comment headers."""
    blocks = re.split(r"-{60,}", sql_text)
    queries: list[tuple[str, str]] = []
    for block in blocks:
        lines = block.strip().splitlines()
        label = None
        sql_lines = []
        for line in lines:
            m = re.match(r"--\s*(Q\d+:\s*.+)", line.strip())
            if m:
                label = m.group(1).strip()
            elif line.strip() and not line.strip().startswith("--"):
                sql_lines.append(line)
        sql = "\n".join(sql_lines).strip()
        if label and sql:
            queries.append((label, sql))
    return queries


def run_benchmark(
    con: snowflake.connector.SnowflakeConnection,
    queries: list[tuple[str, str]],
    n_runs: int,
) -> list[dict]:
    cur = con.cursor()
    # Disable result cache for fair benchmarking (measurement_concept.md requirement)
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")

    results = []
    for label, sql in queries:
        print(f"  Benchmarking: {label} ({n_runs} runs)...")
        run_times: list[float] = []

        for i in range(n_runs):
            t0 = time.perf_counter()
            cur.execute(sql)
            cur.fetchall()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            run_times.append(elapsed_ms)
            cold_warm = "cold" if i == 0 else "warm"
            print(f"    Run {i+1:02d} ({cold_warm}): {elapsed_ms:.1f} ms")

        results.append({
            "query_label": label,
            "n_runs": n_runs,
            "mean_ms": round(statistics.mean(run_times), 3),
            "std_ms": round(statistics.stdev(run_times) if n_runs > 1 else 0.0, 3),
            "min_ms": round(min(run_times), 3),
            "max_ms": round(max(run_times), 3),
            "cold_run_ms": round(run_times[0], 3),
            "warm_mean_ms": round(statistics.mean(run_times[1:]), 3) if n_runs > 1 else round(run_times[0], 3),
        })

    cur.close()
    return results


def write_results(results: list[dict], output_dir: Path, volume: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"snowflake_{volume}_results.csv"
    fieldnames = list(results[0].keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    return out_path


def main() -> None:
    args = parse_args()
    queries_path = Path(args.queries)
    output_dir = Path(args.output_dir)

    sql_text = queries_path.read_text(encoding="utf-8")
    queries = parse_queries(sql_text)
    print(f"Loaded {len(queries)} queries from {queries_path}")

    print("\nConnecting to Snowflake...")
    con = get_connection(args.env_file)

    print(f"Running benchmark (volume={args.volume}, {args.runs} runs/query)...\n")
    results = run_benchmark(con=con, queries=queries, n_runs=args.runs)
    con.close()

    out_path = write_results(results, output_dir, volume=args.volume)
    print(f"\nResults written to: {out_path}")

    print("\nSummary:")
    print(f"{'Query':<45} {'Mean (ms)':>10} {'Std (ms)':>10} {'Cold (ms)':>10}")
    print("-" * 80)
    for r in results:
        print(f"{r['query_label']:<45} {r['mean_ms']:>10.1f} {r['std_ms']:>10.1f} {r['cold_run_ms']:>10.1f}")


if __name__ == "__main__":
    main()
