from __future__ import annotations

import argparse
import csv
import re
import statistics
import time
from pathlib import Path

import duckdb


N_RUNS = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run DuckDB KPI benchmark.")
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--queries",
        default="03_embedded_dwh/kpi_queries.sql",
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
    return parser.parse_args()


def parse_queries(sql_text: str) -> list[tuple[str, str]]:
    """
    Finds all Q-label blocks by scanning for Q-comment headers,
    then collecting SQL until the next Q-label or end of file.
    Robust against varying separator line lengths.
    """
    queries: list[tuple[str, str]] = []
    label_pattern = re.compile(r'--\s*(Q\d+:\s*[^\n]+)', re.MULTILINE)
    matches = list(label_pattern.finditer(sql_text))

    for i, match in enumerate(matches):
        label = match.group(1).strip()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(sql_text)
        block = sql_text[start:end]

        sql_lines = []
        for line in block.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith('--'):
                sql_lines.append(line)
        sql = '\n'.join(sql_lines).strip()

        if sql:
            queries.append((label, sql))

    return queries


def run_benchmark(db_path: Path, queries: list[tuple[str, str]], n_runs: int) -> list[dict]:
    con = duckdb.connect(str(db_path), read_only=True)
    results = []

    for label, sql in queries:
        print(f"  Benchmarking: {label} ({n_runs} runs)...")
        run_times: list[float] = []

        for i in range(n_runs):
            t0 = time.perf_counter()
            con.execute(sql).fetchall()
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

    con.close()
    return results


def write_results(results: list[dict], output_dir: Path, volume: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"duckdb_{volume}_results.csv"

    fieldnames = list(results[0].keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    return out_path


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    queries_path = Path(args.queries)
    output_dir = Path(args.output_dir)

    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")

    sql_text = queries_path.read_text(encoding="utf-8")
    queries = parse_queries(sql_text)
    print(f"Loaded {len(queries)} queries from {queries_path}")

    if not queries:
        raise ValueError(
            f"No queries found in {queries_path}. "
            "Ensure queries are labeled with '-- Q1: ...' style comments."
        )

    print(f"\nRunning benchmark on {db_path} (volume={args.volume}, {args.runs} runs/query)...\n")
    results = run_benchmark(db_path=db_path, queries=queries, n_runs=args.runs)

    out_path = write_results(results, output_dir, volume=args.volume)
    print(f"\nResults written to: {out_path}")

    print("\nSummary:")
    print(f"{'Query':<45} {'Mean (ms)':>10} {'Std (ms)':>10} {'Cold (ms)':>10}")
    print("-" * 80)
    for r in results:
        print(f"{r['query_label']:<45} {r['mean_ms']:>10.1f} {r['std_ms']:>10.1f} {r['cold_run_ms']:>10.1f}")


if __name__ == "__main__":
    main()