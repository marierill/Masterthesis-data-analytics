"""
anomaly_detection.py – Platform: Snowflake (Architecture A)
============================================================
Detects anomalies in monthly aggregated revenue using three methods:
  1. Z-Score       – standard deviation-based outlier detection
  2. IQR           – interquartile range-based outlier detection
  3. Isolation Forest – ML-based anomaly detection (sklearn)

Input:  Snowflake BENCHMARK.MAIN database
Output: CSV result file in 05_benchmark_results/

Usage:
  python 04_cloud_dwh/anomaly_detection.py --volume small

Methodological note:
  Logically identical to 03_embedded_dwh/anomaly_detection.py.
  Only the database connection differs between platforms.
  This ensures comparability of anomaly detection results (see thesis ch. 4.3.2).
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
import snowflake.connector


# ---------------------------------------------------------------------------
# Configuration – identical to DuckDB version
# ---------------------------------------------------------------------------

ZSCORE_THRESHOLD = 2.5
IQR_MULTIPLIER = 1.5
ISOLATION_FOREST_CONTAMINATION = 0.1
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

def get_connection(env_file: str = ".env") -> snowflake.connector.SnowflakeConnection:
    load_dotenv(env_file)
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )


def load_monthly_revenue(env_file: str) -> pd.DataFrame:
    """Execute Q13 on Snowflake and return monthly revenue as DataFrame."""
    con = get_connection(env_file)
    cur = con.cursor()
    cur.execute(Q13_MONTHLY_REVENUE)
    rows = cur.fetchall()
    cols = [desc[0].lower() for desc in cur.description]
    cur.close()
    con.close()
    df = pd.DataFrame(rows, columns=cols)
    df["monthly_revenue"] = df["monthly_revenue"].astype(float)
    return df


# ---------------------------------------------------------------------------
# Anomaly detection methods – identical to DuckDB version
# ---------------------------------------------------------------------------

def detect_zscore(df: pd.DataFrame):
    mean = df["monthly_revenue"].mean()
    std = df["monthly_revenue"].std()
    z_scores = (df["monthly_revenue"] - mean) / std
    return z_scores.abs() > ZSCORE_THRESHOLD, z_scores


def detect_iqr(df: pd.DataFrame):
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


def detect_isolation_forest(df: pd.DataFrame):
    X = df["monthly_revenue"].values.reshape(-1, 1)
    model = IsolationForest(
        contamination=ISOLATION_FOREST_CONTAMINATION,
        random_state=RANDOM_SEED,
        n_estimators=100,
    )
    predictions = model.fit_predict(X)
    scores = model.score_samples(X)
    is_anomaly = predictions == -1
    return pd.Series(is_anomaly, index=df.index), pd.Series(scores, index=df.index)


# ---------------------------------------------------------------------------
# Result assembly – identical to DuckDB version
# ---------------------------------------------------------------------------

def run_anomaly_detection(volume: str, env_file: str) -> pd.DataFrame:
    print(f"Loading monthly revenue from Snowflake (volume={volume})...")
    t0 = time.perf_counter()
    df = load_monthly_revenue(env_file)
    load_time = time.perf_counter() - t0
    print(f"  Q13 executed: {len(df)} months loaded in {load_time:.3f}s")

    mean_rev = df["monthly_revenue"].mean()
    std_rev = df["monthly_revenue"].std()
    print(f"  Mean monthly revenue: {mean_rev:,.0f} EUR")
    print(f"  Std deviation:        {std_rev:,.0f} EUR")

    print("\nRunning Z-Score detection...")
    zscore_flags, z_scores = detect_zscore(df)
    print(f"  Anomalies detected: {zscore_flags.sum()} / {len(df)} months")

    print("Running IQR detection...")
    iqr_flags, lower_fence, upper_fence = detect_iqr(df)
    print(f"  Lower fence: {lower_fence:,.0f} EUR")
    print(f"  Upper fence: {upper_fence:,.0f} EUR")
    print(f"  Anomalies detected: {iqr_flags.sum()} / {len(df)} months")

    print("Running Isolation Forest detection...")
    iforest_flags, iforest_scores = detect_isolation_forest(df)
    print(f"  Anomalies detected: {iforest_flags.sum()} / {len(df)} months")

    results = df.copy()
    results["platform"] = "snowflake"
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
    results["consensus_anomaly"] = (
        results["zscore_anomaly"].astype(int) +
        results["iqr_anomaly"].astype(int) +
        results["iforest_anomaly"].astype(int)
    ) >= 2

    return results


def write_results(results: pd.DataFrame, output_dir: Path, volume: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"snowflake_{volume}_anomaly_results.csv"
    results.to_csv(out_path, index=False)
    return out_path


def print_summary(results: pd.DataFrame) -> None:
    print("\n" + "=" * 70)
    print("ANOMALY DETECTION SUMMARY – Snowflake")
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
        description="Anomaly detection on monthly revenue – Snowflake platform."
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
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file with Snowflake credentials.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"\nAnomaly Detection – Platform: Snowflake | Volume: {args.volume}")
    print(f"Account: {os.environ.get('SNOWFLAKE_ACCOUNT', '(from .env)')}\n")

    results = run_anomaly_detection(volume=args.volume, env_file=args.env_file)
    print_summary(results)

    out_path = write_results(results, Path(args.output_dir), args.volume)
    print(f"\nResults written to: {out_path}")


if __name__ == "__main__":
    main()
