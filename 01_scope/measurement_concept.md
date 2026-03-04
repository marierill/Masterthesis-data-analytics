# Measurement Concept

This document defines the benchmarking and evaluation methodology used to compare:

- Architecture A: Cloud Data Warehouse
- Architecture B: DuckDB (local embedded analytics)

The measurement logic is identical across both platforms.

No deviation from this document is allowed once benchmarking begins.

---

# 1. Measurement Objectives

The benchmarking aims to evaluate:

- Query performance
- Storage footprint
- Implementation effort
- Transformation effort
- Time-to-First-Insight
- Maintainability and governance characteristics

All metrics are captured under controlled and reproducible conditions.

---

# 2. Workload Definition

The benchmark workload includes:

1. Simple aggregations (SUM, COUNT)
2. Multi-dimensional GROUP BY queries
3. Window function queries (MoM, rolling average)
4. Ranking queries (Top-N share)
5. Time-series aggregation
6. Python-based anomaly detection (monthly aggregated revenue)

Each workload type is executed identically on both platforms.

---

# 3. Data Volume Levels

Measurements are performed for:

- 500,000 rows
- 5,000,000 rows
- 20,000,000 rows

All volume levels use identical schema and statistical distributions.

---

# 4. Performance Measurement

## 4.1 Query Execution Measurement

For each KPI query:

- Execute 10 consecutive runs
- Capture execution time per run
- Calculate:
  - Mean execution time
  - Standard deviation
  - Minimum and maximum runtime

Cold run and warm run must be distinguished where applicable.

Definition:

Cold run = first execution after cache reset or warehouse restart  
Warm run = subsequent execution without environment reset

If cache reset is not technically possible, this must be documented.

---

## 4.2 Measurement Granularity

Execution time is measured:

- At SQL execution level (database runtime)
- Not including visualization rendering time
- Python overhead must be separated from SQL runtime

---

# 5. Storage Measurement

For each dataset and platform:

- Measure total storage footprint
- Include:
  - Fact table storage
  - Dimension tables
  - Indexes (if used)
  - Metadata if relevant

Units must be standardized (MB / GB).

For cloud platforms:
- Logical storage size
- Compressed size (if available)

---

# 6. Implementation Effort

Measured qualitatively and quantitatively.

## 6.1 Setup Time

Record:

- Initial environment setup duration
- Schema deployment time
- Data loading time

## 6.2 SQL Adaptation Effort

Document:

- Number of syntax adjustments required
- Platform-specific rewrites
- Compatibility issues

---

# 7. Transformation Effort

Evaluate:

- Complexity of data ingestion
- Data type conversions
- Partitioning / clustering setup
- Required preprocessing steps

Document deviations between platforms.

---

# 8. Time-to-First-Insight

Definition:

Time from environment readiness to first successfully executed KPI query.

Includes:

- Schema creation
- Data ingestion
- First validated KPI output

Measured once per architecture.

---

# 9. Reproducibility Rules

To ensure validity:

- All benchmark scripts must be version-controlled.
- Hardware specifications must be documented.
- Cloud warehouse size must be fixed during benchmarking.
- No dynamic scaling during measurement.
- No manual query tuning between runs.

All environment configurations must be recorded in:

benchmark_environment.md

---

# 10. Statistical Treatment

For each query and volume:

Report:

- Mean runtime
- Standard deviation
- Relative difference between architectures
- Scaling factor (runtime increase between volume levels)

Scaling behavior should be evaluated:

- Linear
- Sub-linear
- Super-linear

Visualization:

- Runtime vs volume plots
- Log-scale optional

---

# 11. Qualitative Evaluation Dimensions

In addition to numeric metrics, evaluate:

- Maintainability
- Governance capability
- Organizational scalability
- Operational complexity
- Lock-in risk

Qualitative evaluation must be clearly separated from quantitative results.

---

# 12. Measurement Integrity Constraints

The following actions are prohibited:

- Query rewrites that change logical complexity
- Platform-specific indexing not mirrored on both systems
- Additional materialized views
- Caching tricks benefiting only one architecture
- Modifying data distributions after benchmarking starts

---

# 13. Benchmark Output Artifacts

The following files must be produced:

- Raw benchmark results (CSV/Parquet)
- Aggregated summary results
- Plots (runtime vs volume)
- Evaluation notebook
- Environment documentation

---

# 14. Measurement Limitations

This benchmark does not measure:

- Real-time performance
- Multi-user concurrency
- Enterprise-scale cluster distribution
- Advanced ML workloads
- Production SLAs

The benchmark reflects controlled BI workloads only.