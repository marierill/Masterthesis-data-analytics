# Benchmark Environment Documentation

This document defines the technical environment under which all benchmarking
measurements were executed.

Its purpose is to ensure:
- Reproducibility
- Transparency
- Methodological validity
- Separation of architecture effects from hardware effects

---

# 1. Local System Environment (DuckDB)

## 1.1 Operating System

- OS Name: Windows 11 Home
- OS Version: 25H2, Build 26200.8037
- Architecture (x64 / ARM): x64

## 1.2 Hardware Specifications

- Device: Huawei MateBook D 15
- CPU Model: Intel Core i3-1115G4, 11th Gen, 3.00 GHz (Boost 4.10 GHz)
- Number of Physical Cores: 2
- Number of Logical Threads: 4 (Hyper-Threading enabled)
- Base Frequency: 3.00 GHz
- RAM (GB): 8.00 GB (7.80 GB usable)
- Storage Type: PCIe SSD ("PCIe-8 SSD 256 GB")
- Available Disk Space at Benchmark Start: ~6 GB free (after CSV cleanup)

## 1.3 Software Stack

- Python Version: 3.11.8
- DuckDB Version: 1.1.3
- Pandas Version: 2.2.3
- PyArrow Version: 17.0.0
- Matplotlib Version: 3.9.2
- scikit-learn Version: 1.5.2
- dbt-duckdb Version: 1.10.1
- dbt-core Version: 1.11.8
- Virtual Environment Tool: venv (Python built-in, .venv/ in repo root)

## 1.4 DuckDB Configuration

- In-memory or file-based mode: File-based (.duckdb file per volume)
- Threads configured: 4 (default, all logical threads)
- PRAGMA settings (if modified): None – default configuration
- Temporary directory location: System default (Windows TEMP)
- DuckDB files:
  - data/generated/small/benchmark.duckdb   (500k rows)
  - data/generated/medium/benchmark.duckdb  (5M rows)
  - data/generated/large/benchmark.duckdb   (20M rows)

---

# 2. Cloud Data Warehouse Environment

## 2.1 Provider Information

- Provider Name: Snowflake (Free Trial, Standard Edition)
- Account Identifier: wmmawvo-xb22291
- Region: AWS EU (Frankfurt) – eu-central-1
- Warehouse Name: BENCHMARK_WH
- Warehouse Size / Compute Tier: X-Small (XS) – 8 vCPUs, ~16 GB RAM
- Auto-scaling Enabled: No (single cluster, fixed size)
- Multi-cluster Enabled: No
- Auto-Suspend: 60 seconds after last query
- Auto-Resume: Yes (automatic on query submission)

## 2.2 Storage Configuration

- Storage Model: Separate compute and storage (Snowflake native architecture)
- Database: BENCHMARK
- Schema: MAIN
- Compression Enabled: Yes (Snowflake automatic micro-partition compression)
- Data Format: Parquet ingestion via internal stage (PUT + COPY INTO)
  - DATE columns loaded via two-step VARIANT staging workaround
    (Pandas Parquet stores DATE as int64 nanoseconds; direct COPY INTO
    cannot cast to DATE – see load_data.py for implementation details)

## 2.3 Session & Performance Settings

- Result Caching: Disabled (ALTER SESSION SET USE_CACHED_RESULT = FALSE)
  applied automatically at start of every benchmark session
- Query Acceleration Service: Not enabled (XS warehouse, Free Trial)
- Concurrency Level During Tests: 1 (single user, single warehouse)
- Manual Scaling Performed: No
- Network Policy: Removed during testing to allow browser access
  (ALTER ACCOUNT UNSET NETWORK_POLICY)

---

# 3. Dataset Configuration

## 3.1 Dataset Volumes

All three volumes were benchmarked on both platforms:

| Label  | Fact Rows  | Purpose                        |
|--------|------------|--------------------------------|
| small  | 500,000    | Smoke test, logic validation   |
| medium | 5,000,000  | Primary benchmark              |
| large  | 20,000,000 | Scalability stress test        |

Dimension tables (identical across all volumes):
- dim_date: 731 rows (Jan 2024 – Dec 2025)
- dim_product: 60 rows
- dim_customer: 15,000 rows
- dim_region: 12 rows
- dim_costcenter: 18 rows

Note (Snowflake): Snowflake uses a single shared database (BENCHMARK.MAIN).
Each volume was loaded sequentially (DROP + CREATE + load) before its
benchmark run. DuckDB uses separate .duckdb files per volume.

## 3.2 File Format

- Primary format: Parquet (Snappy compression) – used for all benchmark loads
- Secondary format: CSV (uncompressed) – generated for transparency,
  deleted before large-volume benchmarking to free disk space
- Partitioning Strategy: None (single file per table per volume)
- Data Generation Seed: 42 (base) + volume offset
  - small:  seed = 53 (42 + 11)
  - medium: seed = 59 (42 + 17)
  - large:  seed = 65 (42 + 23)
- Generator: 02_data_generation/generator.py (chunked Parquet writing,
  chunk size 2,000,000 rows to prevent OOM on 8 GB RAM)

---

# 4. Benchmark Execution Configuration

## 4.1 Query Execution Rules

- Number of runs per query: 10
- Cold run: Run 01 (first execution after fresh connection)
- Warm runs: Runs 02–10 (subsequent executions, same connection)
- Cold run reset method:
  - DuckDB: fresh duckdb.connect() call per benchmark session
  - Snowflake: fresh snowflake.connector.connect() call per session;
    result cache disabled via ALTER SESSION SET USE_CACHED_RESULT = FALSE
- Warm run definition: Any run after the first within the same connection
- Query timeout settings: None configured (system defaults)
- Parallel query execution: No (sequential, one query at a time)

## 4.2 Measurement Method

- Timing method: Python time.perf_counter() at application level
- Time unit: Milliseconds (ms)
- Overhead included:
  - DuckDB: Python function call overhead only (~0.01 ms, negligible)
  - Snowflake: Network round-trip latency to AWS EU Frankfurt included
    (measured overhead: ~80–120 ms per query at application level)
- Overhead excluded: Result rendering, Power BI visualization time
- Note: Snowflake internal query execution time (excluding network) is
  available via QUERY_HISTORY view but was not used as primary metric
  to ensure methodological consistency between platforms

## 4.3 Logging

- DuckDB results: 05_benchmark_results/duckdb_{volume}_results.csv
- Snowflake results: 05_benchmark_results/snowflake_{volume}_results.csv
- Anomaly results: 05_benchmark_results/{platform}_{volume}_anomaly_results.csv
- Result format: CSV with columns:
  query_label, n_runs, mean_ms, std_ms, min_ms, max_ms,
  cold_run_ms, warm_mean_ms
- Timestamp format: ISO 8601 (file creation timestamp)
- Plots: 05_benchmark_results/plots/*.png (150 DPI)

---

# 5. Time-to-First-Insight Measurement

## Architecture B: DuckDB

- Environment setup start: 14.04.2026 (Phase 0 completion)
- Schema deployment completion: 14.04.2026 (schema.sql via load_data.py)
- Data load completion (small): 14.04.2026, ~3.77s for fact_billing_lines
- First successful KPI execution: 14.04.2026
  Query: SELECT COUNT(*), SUM(revenue) FROM fact_billing_lines
  Result: 500,000 rows | 557,734,251.83 EUR total revenue
- First dbt mart available: 15.04.2026 (dbt run: 2.13s, PASS=3)
- First Power BI dashboard: 16.04.2026 (after ODBC driver installation)
- Total duration (setup to dashboard): ~2 days (distributed work sessions)

## Architecture A: Snowflake

- Snowflake registration: 21.04.2026
- Schema deployment completion: 21.04.2026 (Python-based SQL parser)
- Data load completion (small): 21.04.2026, ~11.48s for fact_billing_lines
- First successful KPI execution: 21.04.2026
  Query: SELECT CURRENT_VERSION() → 10.13.104
- First dbt mart available: 21.04.2026 (dbt run --target snowflake: 6.85s)
- First Power BI dashboard: 22.04.2026 (native Snowflake connector)
- Total duration (registration to dashboard): ~2 days (distributed work)

---

# 6. Known Constraints During Measurement

- Hardware RAM limitation: 8 GB RAM caused OOM crash during initial
  large-volume data generation. Resolved by implementing chunked Parquet
  writing (2M rows per chunk) in generator.py.
- Snowflake DATE casting: Pandas Parquet stores DATE columns as int64
  nanoseconds. Snowflake COPY INTO cannot cast directly to DATE.
  Resolved via two-step VARIANT staging workaround in load_data.py.
- Snowflake ODBC not applicable: DuckDB ODBC installer failed due to
  missing VC++ Redistributable. Resolved by manual Windows Registry
  registration of DuckDB ODBC driver DLLs.
- Power BI DuckDB connection: access_mode=read_only required in ODBC
  connection string due to DuckDB single-writer constraint.
- Snowflake Network Policy: IP-based access restriction activated after
  initial setup. Resolved by removing account-level network policy via
  Python connector (ALTER ACCOUNT UNSET NETWORK_POLICY).
- Snowflake result caching: Disabled via USE_CACHED_RESULT = FALSE.
  Without this setting, warm runs would return cached results in ~10ms
  regardless of actual computation time, invalidating measurements.
- Network jitter: Snowflake measurements include variable network latency
  (AWS EU Frankfurt). High standard deviation in small-volume benchmarks
  (~271ms for Q1 small) reflects network conditions, not platform instability.
- Background processes: Standard Windows background services running
  during all measurements. No active media playback or heavy applications.
- Thermal throttling: Not observed (i3-1115G4 TDP 28W, benchmark
  workloads do not sustain maximum thermal load).
- Manual intervention: None during benchmark execution. All runs completed
  automatically via benchmark.py scripts.

---

# 7. Version Control Reference

- Repository: https://github.com/marierill/Masterthesis-data-analytics
- Branch: feat_first_test
- DuckDB benchmark commit: a74d243
- Snowflake benchmark commit: 1f1d22a
- Date of DuckDB measurement: 16.04.2026
- Date of Snowflake measurement: 21.04.2026–23.04.2026

All benchmark result CSVs are committed and traceable to the above commits.

---

# 8. Integrity Declaration

All benchmarks were executed:

- Without logical query changes between architectures
  (only syntax adjustments where required by platform; see kpi_queries.sql)
- Without selective optimization benefiting only one platform
  (no indexing, no materialized views, no query hints)
- Without changing dataset distributions mid-benchmark
  (fixed seed 42 + volume offsets; validated via validate_data.py)
- With result caching disabled on Snowflake
  (USE_CACHED_RESULT = FALSE enforced per session)
- With identical SQL workload definition
  (03_embedded_dwh/kpi_queries.sql and 04_cloud_dwh/kpi_queries.sql
  are logically equivalent; only platform-specific syntax differs)

Deviations from ideal conditions:
- Snowflake measurements include network latency (unavoidable in cloud setup)
- Local hardware (i3, 8 GB RAM) is below typical production server spec
- Snowflake Free Trial limits warehouse to XS tier
These deviations are documented and discussed in thesis Chapter 5.
