"""
evaluation.py – Benchmark Auswertung und Visualisierung
========================================================
Phase 4: Vergleichende Analyse DuckDB vs. Snowflake

Erstellt folgende Outputs in 05_benchmark_results/plots/:
  1. warm_mean_comparison.png  – Warm Mean pro Query und Volume (Balken)
  2. scaling_behavior.png      – Skalierungsverhalten über Volumes (Linien)
  3. cold_warm_ratio.png       – Cold/Warm-Verhältnis DuckDB vs. Snowflake
  4. std_comparison.png        – Streuung (Standardabweichung) im Vergleich
  5. crossover_analysis.png    – DuckDB/SF Faktor pro Query (Crossover-Punkt)
  6. tco_comparison.png        – TCO-Übersicht DuckDB vs. Snowflake

Usage:
  python 05_benchmark_results/evaluation.py
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RESULTS_DIR = Path("05_benchmark_results")
PLOTS_DIR = RESULTS_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# Colors
COLOR_DUCKDB = "#1B6CA8"
COLOR_SF = "#29B5E8"
COLOR_DUCKDB_LIGHT = "#A8CDE8"
COLOR_SF_LIGHT = "#A8E4F5"
COLOR_HIGHLIGHT = "#E8433A"

VOLUMES = ["small", "medium", "large"]
VOLUME_LABELS = {"small": "Small\n(500k)", "medium": "Medium\n(5M)", "large": "Large\n(20M)"}

QUERY_SHORT = {
    "Q1: Total Revenue": "Q1\nTotal Rev",
    "Q2: Total Cost": "Q2\nTotal Cost",
    "Q3: Contribution Margin + Margin Ratio": "Q3\nMargin",
    "Q4: Monthly Recurring Revenue (MRR)": "Q4\nMRR",
    "Q5: Average Revenue per Account (ARPA)": "Q5\nARPA",
    "Q6: Revenue by Plan Tier": "Q6\nPlan Tier",
    "Q7: Revenue by Customer Segment": "Q7\nSegment",
    "Q8: Revenue Growth Month-over-Month (MoM)": "Q8\nMoM",
    "Q9: Revenue Growth Year-over-Year (YoY)": "Q9\nYoY",
    "Q10: Rolling 3-Month Revenue Average": "Q10\nRolling Avg",
    "Q11: Cumulative Revenue Year-to-Date (YTD)": "Q11\nYTD",
    "Q12: Revenue Concentration - Top-10% Customer Share": "Q12\nTop-10%",
    "Q13: Monthly Aggregated Revenue (Input for Anomaly Detection)": "Q13\nAnomaly",
}

CATEGORY_COLORS = {
    "Simple Aggregation": "#2E86AB",
    "Filtered Aggregation": "#A23B72",
    "Multi-dim. GROUP BY": "#F18F01",
    "Window Function": "#C73E1D",
    "Ranking": "#3B1F2B",
    "Time-Series": "#44BBA4",
}

QUERY_CATEGORIES = {
    "Q1: Total Revenue": "Simple Aggregation",
    "Q2: Total Cost": "Simple Aggregation",
    "Q3: Contribution Margin + Margin Ratio": "Simple Aggregation",
    "Q4: Monthly Recurring Revenue (MRR)": "Filtered Aggregation",
    "Q5: Average Revenue per Account (ARPA)": "Filtered Aggregation",
    "Q6: Revenue by Plan Tier": "Multi-dim. GROUP BY",
    "Q7: Revenue by Customer Segment": "Multi-dim. GROUP BY",
    "Q8: Revenue Growth Month-over-Month (MoM)": "Window Function",
    "Q9: Revenue Growth Year-over-Year (YoY)": "Window Function",
    "Q10: Rolling 3-Month Revenue Average": "Window Function",
    "Q11: Cumulative Revenue Year-to-Date (YTD)": "Window Function",
    "Q12: Revenue Concentration - Top-10% Customer Share": "Ranking",
    "Q13: Monthly Aggregated Revenue (Input for Anomaly Detection)": "Time-Series",
}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_results() -> dict[tuple[str, str], pd.DataFrame]:
    datasets = {}
    for platform in ["duckdb", "snowflake"]:
        for volume in VOLUMES:
            path = RESULTS_DIR / f"{platform}_{volume}_results.csv"
            if path.exists():
                df = pd.read_csv(path)
                df["platform"] = "DuckDB" if platform == "duckdb" else "Snowflake"
                df["volume"] = volume
                df["category"] = df["query_label"].map(QUERY_CATEGORIES)
                datasets[(platform, volume)] = df
            else:
                print(f"WARNING: {path} not found – skipping")
    return datasets


def build_combined(datasets: dict) -> pd.DataFrame:
    return pd.concat(datasets.values(), ignore_index=True)


# ---------------------------------------------------------------------------
# Plot 1: Warm Mean Comparison per Volume
# ---------------------------------------------------------------------------

def plot_warm_mean_comparison(datasets: dict) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(18, 7), sharey=False)
    fig.suptitle(
        "Warm-Mean-Laufzeit DuckDB vs. Snowflake nach Query und Volumen",
        fontsize=13, fontweight="bold", y=1.02
    )

    for ax, volume in zip(axes, VOLUMES):
        dk = datasets.get(("duckdb", volume))
        sf = datasets.get(("snowflake", volume))
        if dk is None or sf is None:
            ax.set_title(f"{volume} (Daten fehlen)")
            continue

        queries = dk["query_label"].tolist()
        x = np.arange(len(queries))
        width = 0.38

        bars_dk = ax.bar(x - width/2, dk["warm_mean_ms"], width,
                         label="DuckDB", color=COLOR_DUCKDB, alpha=0.9, zorder=3)
        bars_sf = ax.bar(x + width/2, sf["warm_mean_ms"], width,
                         label="Snowflake", color=COLOR_SF, alpha=0.9, zorder=3)

        # Highlight crossover
        for i, (d, s) in enumerate(zip(dk["warm_mean_ms"], sf["warm_mean_ms"])):
            if s < d:
                ax.bar(x[i] - width/2, d, width, color=COLOR_HIGHLIGHT, alpha=0.3, zorder=4)
                ax.bar(x[i] + width/2, s, width, color=COLOR_HIGHLIGHT, alpha=0.3, zorder=4)

        ax.set_title(
            f"{VOLUME_LABELS[volume]}",
            fontsize=12, fontweight="bold"
        )
        ax.set_xticks(x)
        ax.set_xticklabels(
            [QUERY_SHORT.get(q, q) for q in queries],
            fontsize=7, rotation=0, ha="center"
        )
        ax.set_ylabel("Laufzeit Warm Mean (ms)", fontsize=10)
        ax.grid(axis="y", alpha=0.3, zorder=0)
        ax.legend(fontsize=9)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.tight_layout()
    path = PLOTS_DIR / "warm_mean_comparison.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Plot 2: Scaling Behavior
# ---------------------------------------------------------------------------

def plot_scaling_behavior(datasets: dict) -> None:
    # Select representative queries from each category
    selected = [
        "Q1: Total Revenue",
        "Q5: Average Revenue per Account (ARPA)",
        "Q7: Revenue by Customer Segment",
        "Q8: Revenue Growth Month-over-Month (MoM)",
        "Q12: Revenue Concentration - Top-10% Customer Share",
        "Q13: Monthly Aggregated Revenue (Input for Anomaly Detection)",
    ]

    fig, axes = plt.subplots(2, 3, figsize=(16, 10))
    fig.suptitle(
        "Skalierungsverhalten DuckDB vs. Snowflake (Warm Mean ms)",
        fontsize=13, fontweight="bold", y=1.01
    )
    axes = axes.flatten()
    volume_points = [0.5, 5, 20]  # Millionen Zeilen

    for ax, query in zip(axes, selected):
        dk_vals, sf_vals = [], []
        for volume in VOLUMES:
            dk = datasets.get(("duckdb", volume))
            sf = datasets.get(("snowflake", volume))
            if dk is not None:
                row = dk[dk["query_label"] == query]
                dk_vals.append(row["warm_mean_ms"].values[0] if len(row) else None)
            if sf is not None:
                row = sf[sf["query_label"] == query]
                sf_vals.append(row["warm_mean_ms"].values[0] if len(row) else None)

        ax.plot(volume_points, dk_vals, "o-", color=COLOR_DUCKDB,
                linewidth=2.5, markersize=7, label="DuckDB", zorder=3)
        ax.plot(volume_points, sf_vals, "s--", color=COLOR_SF,
                linewidth=2.5, markersize=7, label="Snowflake", zorder=3)

        # Crossover shading
        dk_arr = np.array(dk_vals, dtype=float)
        sf_arr = np.array(sf_vals, dtype=float)
        if np.any(sf_arr < dk_arr):
            ax.fill_between(volume_points, dk_arr, sf_arr,
                            where=sf_arr < dk_arr,
                            alpha=0.15, color=COLOR_HIGHLIGHT,
                            label="SF schneller")

        short = QUERY_SHORT.get(query, query).replace("\n", " ")
        cat = QUERY_CATEGORIES.get(query, "")
        ax.set_title(f"{short}\n({cat})", fontsize=9, fontweight="bold")
        ax.set_xlabel("Datenmenge (Mio. Zeilen)", fontsize=9)
        ax.set_ylabel("Warm Mean (ms)", fontsize=9)
        ax.set_xticks(volume_points)
        ax.set_xticklabels(["0,5M", "5M", "20M"])
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.tight_layout()
    path = PLOTS_DIR / "scaling_behavior.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Plot 3: Cold/Warm Ratio
# ---------------------------------------------------------------------------

def plot_cold_warm_ratio(datasets: dict) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        "Cold/Warm-Verhältnis DuckDB vs. Snowflake",
        fontsize=13, fontweight="bold"
    )

    for ax, (platform_key, platform_label, color) in zip(
        axes,
        [("duckdb", "DuckDB", COLOR_DUCKDB), ("snowflake", "Snowflake", COLOR_SF)]
    ):
        ratios_by_volume = {}
        for volume in VOLUMES:
            df = datasets.get((platform_key, volume))
            if df is not None:
                ratio = df["cold_run_ms"] / df["warm_mean_ms"].replace(0, np.nan)
                ratios_by_volume[volume] = ratio.values

        queries = datasets.get((platform_key, "small"))["query_label"].tolist()
        x = np.arange(len(queries))
        width = 0.25
        colors_v = ["#1B3A5C", "#2E6BA8", "#5FA8E8"]

        for i, (volume, color_v) in enumerate(zip(VOLUMES, colors_v)):
            if volume in ratios_by_volume:
                ax.bar(x + i * width, ratios_by_volume[volume], width,
                       label=VOLUME_LABELS[volume].replace("\n", " "),
                       color=color_v, alpha=0.85, zorder=3)

        ax.axhline(y=1, color="red", linestyle="--", alpha=0.5, linewidth=1,
                   label="Cold = Warm (Faktor 1)")
        ax.set_title(f"{platform_label}", fontsize=12, fontweight="bold")
        ax.set_xticks(x + width)
        ax.set_xticklabels(
            [QUERY_SHORT.get(q, q) for q in queries],
            fontsize=7, rotation=0, ha="center"
        )
        ax.set_ylabel("Cold/Warm-Faktor", fontsize=10)
        ax.legend(fontsize=8)
        ax.grid(axis="y", alpha=0.3, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.tight_layout()
    path = PLOTS_DIR / "cold_warm_ratio.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Plot 4: Crossover Analysis
# ---------------------------------------------------------------------------

def plot_crossover_analysis(datasets: dict) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle(
        "Laufzeit-Verhältnis Snowflake/DuckDB (Werte <1 = Snowflake schneller)",
        fontsize=13, fontweight="bold"
    )

    for ax, volume in zip(axes, VOLUMES):
        dk = datasets.get(("duckdb", volume))
        sf = datasets.get(("snowflake", volume))
        if dk is None or sf is None:
            continue

        ratios = sf["warm_mean_ms"].values / dk["warm_mean_ms"].replace(0, np.nan).values
        queries = dk["query_label"].tolist()
        colors = [COLOR_HIGHLIGHT if r < 1 else COLOR_DUCKDB for r in ratios]

        ax.barh(range(len(queries)), ratios, color=colors, alpha=0.85, zorder=3)
        ax.axvline(x=1, color="black", linestyle="--", linewidth=1.5,
                   label="Gleichstand (Faktor 1)")
        ax.set_yticks(range(len(queries)))
        ax.set_yticklabels(
            [QUERY_SHORT.get(q, q).replace("\n", " ") for q in queries],
            fontsize=8
        )
        ax.set_xlabel("SF/DK Laufzeit-Verhältnis", fontsize=10)
        ax.set_title(f"{VOLUME_LABELS[volume]}", fontsize=11, fontweight="bold")
        ax.grid(axis="x", alpha=0.3, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        dk_patch = mpatches.Patch(color=COLOR_DUCKDB, label="DuckDB schneller")
        sf_patch = mpatches.Patch(color=COLOR_HIGHLIGHT, label="Snowflake schneller")
        ax.legend(handles=[dk_patch, sf_patch], fontsize=8)

    plt.tight_layout()
    path = PLOTS_DIR / "crossover_analysis.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Plot 5: TCO Comparison
# ---------------------------------------------------------------------------

def plot_tco_comparison() -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        "TCO-Übersicht DuckDB vs. Snowflake (Praxisprojekt)",
        fontsize=13, fontweight="bold"
    )

    # --- Left: Credit consumption ---
    ax = axes[0]
    categories = ["BENCHMARK_WH\n(Benchmark +\nDatenladen)", "COMPUTE_WH\n(Standard)"]
    values = [0.8, 0.2]
    colors = [COLOR_DUCKDB, COLOR_SF]
    bars = ax.bar(categories, values, color=colors, alpha=0.85, zorder=3, width=0.5)
    ax.bar_label(bars, labels=[f"{v} Credits\n(≈ ${v*4:.2f})" for v in values],
                 padding=5, fontsize=10, fontweight="bold")
    ax.set_ylabel("Verbrauchte Credits", fontsize=10)
    ax.set_title("Snowflake Credit-Verbrauch\n(Gesamtprojekt)", fontsize=11, fontweight="bold")
    ax.set_ylim(0, 1.5)
    ax.grid(axis="y", alpha=0.3, zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.text(0.5, -0.22, "Gesamtkosten: ~$4,00 für das vollständige Praxisprojekt",
            transform=ax.transAxes, ha="center", fontsize=9, style="italic", color="gray")

    # --- Right: Effort comparison ---
    ax2 = axes[1]
    phases = ["Phase 0\nUmgebung", "Phase 1\nDaten", "Phase 2\nDuckDB", "Phase 3\nSnowflake"]
    with_ki = [45, 90, 195, 205]
    without_ki = [500, 400, 810, 690]
    x = np.arange(len(phases))
    width = 0.35

    ax2.bar(x - width/2, with_ki, width, label="Mit KI (Claude)", color=COLOR_DUCKDB, alpha=0.85)
    ax2.bar(x + width/2, without_ki, width, label="Geschätzt ohne KI", color="#CCCCCC", alpha=0.85)
    ax2.set_ylabel("Zeitaufwand (Minuten)", fontsize=10)
    ax2.set_title("Implementierungsaufwand\nmit vs. ohne KI-Unterstützung", fontsize=11, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels(phases, fontsize=9)
    ax2.legend(fontsize=9)
    ax2.grid(axis="y", alpha=0.3)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    total_ki = sum(with_ki)
    total_no_ki = sum(without_ki)
    ax2.text(0.5, -0.18,
             f"Gesamt mit KI: {total_ki} min ({total_ki/60:.1f}h) | "
             f"Ohne KI geschätzt: {total_no_ki} min ({total_no_ki/60:.1f}h) | "
             f"Effizienzgewinn: {(1-total_ki/total_no_ki)*100:.0f}%",
             transform=ax2.transAxes, ha="center", fontsize=8, style="italic", color="gray")

    plt.tight_layout()
    path = PLOTS_DIR / "tco_comparison.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame) -> None:
    print("\n" + "="*70)
    print("BENCHMARK SUMMARY – DuckDB vs. Snowflake")
    print("="*70)

    for volume in VOLUMES:
        volume_rows = {'small':'500k','medium':'5M','large':'20M'}[volume]
        print(f"\n--- {volume.upper()} ({volume_rows} Zeilen) ---")
        dk = df[(df["platform"] == "DuckDB") & (df["volume"] == volume)]
        sf = df[(df["platform"] == "Snowflake") & (df["volume"] == volume)]
        if dk.empty or sf.empty:
            continue

        print(f"{'Query':<20} {'DK Warm':>10} {'SF Warm':>10} {'SF/DK':>8}")
        print("-" * 52)
        for _, row in dk.iterrows():
            q_short = row["query_label"][:20]
            dk_val = row["warm_mean_ms"]
            sf_row = sf[sf["query_label"] == row["query_label"]]
            if sf_row.empty:
                continue
            sf_val = sf_row["warm_mean_ms"].values[0]
            ratio = sf_val / dk_val if dk_val > 0 else 0
            marker = " ← SF FASTER" if ratio < 1.0 else ""
            print(f"{q_short:<20} {dk_val:>10.1f} {sf_val:>10.1f} {ratio:>8.1f}x{marker}")

    print("\n" + "="*70)
    print("CROSSOVER QUERIES (Snowflake faster than DuckDB):")
    for volume in VOLUMES:
        dk = df[(df["platform"] == "DuckDB") & (df["volume"] == volume)]
        sf = df[(df["platform"] == "Snowflake") & (df["volume"] == volume)]
        for _, row in dk.iterrows():
            sf_row = sf[sf["query_label"] == row["query_label"]]
            if sf_row.empty:
                continue
            if sf_row["warm_mean_ms"].values[0] < row["warm_mean_ms"]:
                print(f"  {volume:8} | {row['query_label'][:50]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Loading benchmark results...")
    datasets = load_results()

    if not datasets:
        print("ERROR: No result files found in 05_benchmark_results/")
        print("Make sure duckdb_*_results.csv and snowflake_*_results.csv exist.")
        return

    df = build_combined(datasets)
    print(f"Loaded {len(df)} records from {len(datasets)} files.")

    print_summary(df)

    print("\nGenerating plots...")
    plot_warm_mean_comparison(datasets)
    plot_scaling_behavior(datasets)
    plot_cold_warm_ratio(datasets)
    plot_crossover_analysis(datasets)
    plot_tco_comparison()

    print(f"\nAll plots saved to: {PLOTS_DIR}/")
    print("Done.")


if __name__ == "__main__":
    main()
