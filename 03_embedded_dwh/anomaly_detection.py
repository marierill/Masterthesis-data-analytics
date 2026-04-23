"""
anomaly_detection.py – Platform: DuckDB (Architecture B)
=========================================================
Detects anomalies in monthly aggregated revenue using three methods:
  1. Z-Score       – standard deviation-based outlier detection
  2. IQR           – interquartile range-based outlier detection
  3. Isolation Forest – ML-based anomaly detection (sklearn)

Input:  DuckDB database file (benchmark.duckdb), any volume level
Output: CSV result file in 05_benchmark_results/

Usage:
  python 03_embedded_dwh/anomaly_detection.py --db-path data/generated/small/benchmark.duckdb --volume small

Methodological note:
  Anomaly detection runs on Q13 (monthly aggregated revenue) – the same
  query used in the benchmark. This links the analytical workflow directly
  to the benchmark workload definition (see 01_scope/kpi_catalog.md).
  The implementation is logically identical to 04_cloud_dwh/anomaly_detection.py.
  Only the database connection differs between platforms.
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Z-Score threshold: values beyond this many standard deviations are anomalies
ZSCORE_THRESHOLD = 2.5

# IQR multiplier: standard Tukey fence (1.5 = mild, 3.0 = extreme outliers)
IQR_MULTIPLIER = 1.5

# Isolation Forest contamination: expected fraction of anomalies in dataset
# Set to match generator config (anomalies.rate = 0.002 at transaction level;
# at monthly aggregation level we expect a small number of affected months)
ISOLATION_FOREST_CONTAMINATION = 0.1

# Random seed for reproducibility
RANDOM_SEED = 42

# Q13: Monthly aggregated revenue – identical to kpi_queries.sql Q13
Q13_MONTHLY_REVENUE = """
SELECT
    d.year,
    d.month,
    SUM(f.revenue) AS monthly_revenue
FROM fact_billing_lines f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month
"""


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def load_monthly_revenue(db_path: Path) -> pd.DataFrame:
    """Execute Q13 and return monthly revenue as DataFrame."""
    con = duckdb.connect(str(db_path), read_only=True)
    df = con.execute(Q13_MONTHLY_REVENUE).df()
    con.close()
    df["monthly_revenue"] = df["monthly_revenue"].astype(float)
    return df


# ---------------------------------------------------------------------------
# Anomaly detection methods
# ---------------------------------------------------------------------------

def detect_zscore(df: pd.DataFrame) -> pd.Series:
    """
    Z-Score method.
    Formula: z = (x - mean) / std
    A month is flagged as anomalous if abs(z) > ZSCORE_THRESHOLD.
    Returns boolean Series (True = anomaly).
    """
    mean = df["monthly_revenue"].mean()
    std = df["monthly_revenue"].std()
    z_scores = (df["monthly_revenue"] - mean) / std
    return z_scores.abs() > ZSCORE_THRESHOLD, z_scores


def detect_iqr(df: pd.DataFrame) -> pd.Series:
    """
    IQR (Interquartile Range) method.
    Lower fence: Q1 - 1.5 * IQR
    Upper fence: Q3 + 1.5 * IQR
    Values outside fences are flagged as anomalies.
    Returns boolean Series (True = anomaly).
    """
    q1 = df["monthly_revenue"].quantile(0.25)
    q3 = df["monthly_revenue"].quantile(0.75)
    iqr = q3 - q1
    lower_fence = q1 - IQR_MULTIPLIER * iqr
    upper_fence = q3 + IQR_MULTIPLIER * iqr
    is_anomaly = (
        (df["monthly_revenue"] < lower_fence) |
        (df["monthly_revenue"] > upper_fence)
    )
    return is_anomaly, lower_fence, upper_fence


def detect_isolation_forest(df: pd.DataFrame) -> pd.Series:
    """
    Isolation Forest method (sklearn).
    Fits an ensemble of isolation trees on the monthly revenue values.
    Points that are easy to isolate (far from the bulk) are anomalies.
    Returns boolean Series (True = anomaly).
    """
    X = df["monthly_revenue"].values.reshape(-1, 1)
    model = IsolationForest(
        contamination=ISOLATION_FOREST_CONTAMINATION,
        random_state=RANDOM_SEED,
        n_estimators=100,
    )
    # predict: -1 = anomaly, 1 = normal
    predictions = model.fit_predict(X)
    scores = model.score_samples(X)
    is_anomaly = predictions == -1
    return pd.Series(is_anomaly, index=df.index), pd.Series(scores, index=df.index)


# ---------------------------------------------------------------------------
# Result assembly
# ---------------------------------------------------------------------------

def run_anomaly_detection(db_path: Path, volume: str) -> pd.DataFrame:
    """
    Run all three anomaly detection methods on monthly revenue from DuckDB.
    Returns a DataFrame with one row per month and columns for each method.
    """
    print(f"Loading monthly revenue from {db_path}...")
    t0 = time.perf_counter()
    df = load_monthly_revenue(db_path)
    load_time = time.perf_counter() - t0
    print(f"  Q13 executed: {len(df)} months loaded in {load_time:.3f}s")

    # Descriptive statistics
    mean_rev = df["monthly_revenue"].mean()
    std_rev = df["monthly_revenue"].std()
    print(f"  Mean monthly revenue: {mean_rev:,.0f} EUR")
    print(f"  Std deviation:        {std_rev:,.0f} EUR")

    # Method 1: Z-Score
    print("\nRunning Z-Score detection...")
    zscore_flags, z_scores = detect_zscore(df)
    print(f"  Anomalies detected: {zscore_flags.sum()} / {len(df)} months")

    # Method 2: IQR
    print("Running IQR detection...")
    iqr_flags, lower_fence, upper_fence = detect_iqr(df)
    print(f"  Lower fence: {lower_fence:,.0f} EUR")
    print(f"  Upper fence: {upper_fence:,.0f} EUR")
    print(f"  Anomalies detected: {iqr_flags.sum()} / {len(df)} months")

    # Method 3: Isolation Forest
    print("Running Isolation Forest detection...")
    iforest_flags, iforest_scores = detect_isolation_forest(df)
    print(f"  Anomalies detected: {iforest_flags.sum()} / {len(df)} months")

    # Assemble results
    results = df.copy()
    results["platform"] = "duckdb"
    results["volume"] = volume
    results["mean_revenue"] = round(mean_rev, 2)
    results["std_revenue"] = round(std_rev, 2)
    results["z_score"] = z_scores.round(4)
    results["zscore_anomaly"] = zscore_flags
    results["iqr_lower_fence"] = round(lower_fence, 2)
    results["iqr_upper_fence"] = round(upper_fence, 2)
    results["iqr_anomaly"] = iqr_flags
    results["iforest_score"] = iforest_scores.round(6)
    results["iforest_anomaly"] = iforest_flags
    # Consensus: flagged by at least 2 of 3 methods
    results["consensus_anomaly"] = (
        results["zscore_anomaly"].astype(int) +
        results["iqr_anomaly"].astype(int) +
        results["iforest_anomaly"].astype(int)
    ) >= 2

    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_results(results: pd.DataFrame, output_dir: Path, volume: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"duckdb_{volume}_anomaly_results.csv"
    results.to_csv(out_path, index=False)
    return out_path


def print_summary(results: pd.DataFrame) -> None:
    print("\n" + "=" * 70)
    print("ANOMALY DETECTION SUMMARY")
    print("=" * 70)
    print(f"{'Month':<12} {'Revenue (EUR)':>16} {'Z-Score':>8} {'Z':>4} {'IQR':>4} {'IF':>4} {'Consensus':>10}")
    print("-" * 70)
    for _, row in results.iterrows():
        month_str = f"{int(row['year'])}-{int(row['month']):02d}"
        z_flag = "✓" if row["zscore_anomaly"] else " "
        iqr_flag = "✓" if row["iqr_anomaly"] else " "
        if_flag = "✓" if row["iforest_anomaly"] else " "
        con_flag = ">>> ANOMALY" if row["consensus_anomaly"] else ""
        print(
            f"{month_str:<12} {row['monthly_revenue']:>16,.0f} "
            f"{row['z_score']:>8.3f} {z_flag:>4} {iqr_flag:>4} {if_flag:>4} {con_flag:>10}"
        )
    print("-" * 70)
    n_consensus = results["consensus_anomaly"].sum()
    print(f"\nConsensus anomalies (≥2 methods): {n_consensus} / {len(results)} months")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Anomaly detection on monthly revenue – DuckDB platform."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file (e.g. data/generated/small/benchmark.duckdb).",
    )
    parser.add_argument(
        "--volume",
        required=True,
        choices=["small", "medium", "large"],
        help="Volume label for result metadata.",
    )
    parser.add_argument(
        "--output-dir",
        default="05_benchmark_results",
        help="Directory for CSV output (default: 05_benchmark_results).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")

    print(f"\nAnomaly Detection – Platform: DuckDB | Volume: {args.volume}")
    print(f"Database: {db_path}\n")

    results = run_anomaly_detection(db_path=db_path, volume=args.volume)
    print_summary(results)

    out_path = write_results(results, Path(args.output_dir), args.volume)
    print(f"\nResults written to: {out_path}")


if __name__ == "__main__":
    main()