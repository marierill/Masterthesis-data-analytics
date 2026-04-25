# Analytics Platform Architecture Comparison
## Cloud Data Warehouse (Snowflake) vs. Local Embedded Analytics (DuckDB)

> **Masterthesis Project** – Marie Rill | M.Sc. Digital Business Management  
> Hochschule Albstadt-Sigmaringen | Supervisor: Prof. Dr. Hubert Kempter  
> Repository: https://github.com/marierill/Masterthesis-data-analytics

---

## What is this?

A reproducible, end-to-end benchmark comparing two BI/analytics platform
architectures under identical workloads:

- **Architecture A:** Snowflake – managed Cloud Data Warehouse (Standard Edition, XS Warehouse, AWS EU Frankfurt)
- **Architecture B:** DuckDB – local embedded analytics engine (file-based, Python-integrated)

Both architectures are evaluated on the same synthetic SaaS dataset,
the same 13 KPI queries, and the same five evaluation dimensions –
producing directly comparable results that inform an evidence-based
decision model for typical KMU BI/analytics scenarios.

---

## Goals

- **Reproducibility:** All datasets are synthetically generated with a fixed seed (42).
  Anyone can clone this repo and reproduce the full benchmark from scratch.

- **Comparability:** Identical SQL logic on both platforms (syntax adjustments only).
  Identical data volumes, identical query categories, identical measurement protocol
  (10 runs per query, cold/warm separated, result caching disabled on Snowflake).

- **Practical relevance:** The SaaS subscription analytics use case reflects
  real-world KMU BI workloads – not synthetic TPC-H-style stress tests.
  Three volume levels (500k / 5M / 20M rows) model startup, growth-stage,
  and analytics-unit scenarios.

- **Transparency:** All design decisions, measurement constraints, and known
  limitations are documented in `05_benchmark_results/benchmark_environment.md`
  and discussed in the thesis.

- **Portfolio quality:** The repository is structured as a professional
  data engineering project – parameterized scripts, version-controlled
  transformation logic (dbt), CI-ready test suite, and full documentation.

---

## Limitations

- **Hardware:** Benchmarks run on a consumer laptop (Intel Core i3-1115G4,
  8 GB RAM, PCIe SSD). Absolute DuckDB runtimes will differ on higher-spec
  hardware. Relative comparisons and scaling behavior are hardware-independent.

- **Network latency included:** Snowflake measurements are taken at the
  Python application level and include network round-trip latency to
  AWS EU Frankfurt (~80–120 ms). Internal Snowflake execution times
  (available via QUERY_HISTORY) are not used as primary metric to ensure
  methodological consistency between platforms.

- **Snowflake tier:** XS Warehouse (8 vCPUs, ~16 GB RAM) is the smallest
  available configuration. Larger warehouses would improve Snowflake
  performance proportionally. XS represents the realistic entry-level
  for a KMU first adopting Snowflake.

- **Single-user scenario:** No concurrent query load. Multi-user performance
  (Snowflake's primary advantage) is evaluated qualitatively, not benchmarked.

- **No streaming / real-time:** Benchmark covers batch and near-real-time
  analytical workloads only. Streaming pipelines are explicitly out of scope.

- **Free Trial constraints:** Snowflake Free Trial (Standard Edition, 30 days,
  $400 credits). Enterprise features (multi-cluster, advanced governance,
  Tri-Secret Secure) are not available and not evaluated.

- **Synthetic data:** All data is generated, not sourced from a real business.
  Statistical distributions are realistic but anomaly injection rate (0.2%)
  is controlled. Production datasets may exhibit different characteristics.

---

## Repository Structure

```
01_scope/                 # Project definition, KPI catalog, measurement concept
02_data_generation/       # Synthetic data generator (config-driven, reproducible)
03_embedded_dwh/          # DuckDB: schema, load, queries, benchmark, anomaly detection
04_cloud_dwh/             # Snowflake: schema, load, queries, benchmark, anomaly detection
05_benchmark_results/     # Result CSVs, plots, benchmark environment documentation
06_dbt/                   # dbt transformation layer (DuckDB + Snowflake targets)
07_powerbi/               # Power BI dashboards (.pbix) for both platforms
```

---

## Key Results

| Metric | DuckDB | Snowflake |
|---|---|---|
| Q1 Total Revenue – small (500k) | **1.0 ms** | 175 ms |
| Q1 Total Revenue – large (20M) | **17 ms** | 151 ms |
| Q5 ARPA – small | **37 ms** | 319 ms |
| Q5 ARPA – large | 1,007 ms | **568 ms** ← Crossover |
| Scaling factor small→large | ~10–30× | ~1–2× |
| Anomaly detection (Feb 2025) | ✓ detected | ✓ detected |
| Snowflake credit consumption | – | 0.8 Credits (~$3.20) |
| Snowflake Free Trial cost | – | ~$4.00 total |

**Key finding:** DuckDB dominates at small and medium volumes (up to 179× faster).
Snowflake becomes competitive at large volumes for parallelization-heavy operations
(COUNT DISTINCT). Snowflake shows near-zero scaling sensitivity – network latency
dominates regardless of data volume.

---

## Quickstart

See [REPRODUCING.md](REPRODUCING.md) for full step-by-step instructions.

```bash
# 1. Setup
git clone https://github.com/marierill/Masterthesis-data-analytics.git
cd Masterthesis-data-analytics
py -3.11 -m venv .venv && .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. Generate data
python 02_data_generation/generator.py --volume small
python 02_data_generation/validate_data.py --data-dir data/generated/small --expected-rows 500000

# 3. DuckDB benchmark
python 03_embedded_dwh/load_data.py --data-dir data/generated/small --db-path data/generated/small/benchmark.duckdb
python 03_embedded_dwh/benchmark.py --db-path data/generated/small/benchmark.duckdb --volume small --output-dir 05_benchmark_results

# 4. Evaluate
python 05_benchmark_results/evaluation.py
```

---

## Use Case

**SaaS Subscription Revenue Analytics** – a fictitious B2B SaaS company,
24 months of billing history (Jan 2024 – Dec 2025).

### Star Schema

```
fact_billing_lines        # revenue, cost, quantity, subscription_type
    ├── dim_date          # 731 rows – daily calendar
    ├── dim_product       # 60 rows  – plan tiers, pricing models
    ├── dim_customer      # 15,000 rows – segments, channels, industries
    ├── dim_region        # 12 rows  – countries, sales areas
    └── dim_costcenter    # 18 rows  – departments, cost types
```

### Data Volumes

| Label | Rows | Purpose |
|---|---|---|
| small | 500,000 | Smoke test, logic validation |
| medium | 5,000,000 | Primary benchmark |
| large | 20,000,000 | Scalability stress test |

### KPI Workload (13 Queries)

Simple aggregations · Filtered aggregations · Multi-dimensional GROUP BY ·
Window functions (MoM, YoY, rolling average, YTD) · Ranking · Time-series

---

## Measurement Protocol

- **10 runs** per query per volume
- **Cold run** (run 1) and **warm runs** (runs 2–10) reported separately
- **Mean ± standard deviation** reported
- Snowflake: `USE_CACHED_RESULT = FALSE` enforced per session
- DuckDB: `time.perf_counter()` at Python application level
- Results: CSV in `05_benchmark_results/`

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.11.8 |
| Embedded analytics | DuckDB 1.1.3 |
| Cloud DWH | Snowflake Standard (Free Trial) |
| Transformation | dbt Core 1.11.8 (dbt-duckdb + dbt-snowflake) |
| BI / Reporting | Power BI Desktop (ODBC for DuckDB, native for Snowflake) |
| Anomaly detection | scikit-learn 1.5.2 (Z-Score, IQR, Isolation Forest) |
| Version control | Git / GitHub |

---

## References

This benchmark design is inspired by and methodologically aligned with:

- **duckdblabs/db-benchmark** – Reproducible Benchmark of Database-like Ops  
  https://github.com/duckdblabs/db-benchmark

- **DuckDB Labs – NYC Taxi Benchmark** – Benchmarking DuckDB with the NYC Taxi Dataset  
  https://duckdb.org/2024/10/16/driving-csv-performance-benchmarking-duckdb-with-the-nyc-taxi-dataset

- **ClickBench** – A Benchmark For Analytical Databases (ClickHouse)  
  https://github.com/ClickHouse/ClickBench

---

## Citation

```
Rill, Marie (2026): Wirtschaftliche und technische Analyse von BI- und
Analytics-Plattformarchitekturen: Cloud Data Warehouses versus DuckDB
als lokale Alternative. M.Sc. Thesis, Hochschule Albstadt-Sigmaringen.
```
