# CLAUDE.md — Project Context for Claude Code

> Single source of truth for Claude Code.
> Read this file completely before making any changes to the repository.
> Last updated: Phase 1 complete / Phase 2A complete (14.04.2026)

---

## Project Summary

**Thesis:** Wirtschaftliche und technische Analyse von BI- und Analytics-Plattformarchitekturen:
Cloud Data Warehouses versus DuckDB als lokale Alternative
**Author:** Marie Rill | M.Sc. Digital Business Management | Hochschule Albstadt-Sigmaringen
**Supervisor:** Prof. Dr. Hubert Kempter
**Repository:** https://github.com/marierill/Masterthesis-data-analytics

**Core question:** Under which conditions is DuckDB a technically and economically viable
alternative to Cloud Data Warehouses (Snowflake) for typical SaaS BI/analytics workloads?

---

## Architecture Comparison

| | Architecture A | Architecture B |
|---|---|---|
| Platform | Snowflake (Free Trial, Standard, Single-Region AWS EU Frankfurt) | DuckDB 1.1.3 (local, embedded, file-based) |
| BI Tool | Power BI Desktop (DirectQuery) | Power BI Desktop (ODBC) |
| Transformation | dbt Core (06_dbt/, Snowflake target) | dbt Core (06_dbt/, DuckDB target) |
| Benchmark | 04_cloud_dwh/benchmark.py | 03_embedded_dwh/benchmark.py |

---

## Use Case: SaaS Subscription Revenue Analytics

Fictitious B2B SaaS company. 24 months history (Jan 2024 – Dec 2025).

### Star Schema (single fact table, five dimensions)

```
fact_billing_lines    # revenue, cost, quantity, subscription_type   proportional to volume
dim_date              # calendar dimension                            731 rows
dim_product           # plan_name, plan_tier, pricing_model          60 rows
dim_customer          # segment, acquisition_channel, industry        15,000 rows
dim_region            # country, region, sales_area                  12 rows
dim_costcenter        # department, cost_type                        18 rows
```

### Data Volumes

| Label | Rows | Purpose |
|---|---|---|
| small | 500,000 | Smoke test, logic validation |
| medium | 5,000,000 | Primary benchmark |
| large | 20,000,000 | Scalability stress test |

Generator seed: 42 (offsets per volume: small=53, medium=59, large=65)

---

## KPI Workload (13 Queries — defined in kpi_queries.sql)

| ID | KPI | Category |
|---|---|---|
| Q1 | Total Revenue | Simple aggregation |
| Q2 | Total Cost | Simple aggregation |
| Q3 | Contribution Margin + Margin Ratio | Simple aggregation |
| Q4 | Monthly Recurring Revenue (MRR) | Filtered aggregation |
| Q5 | Average Revenue per Account (ARPA) | Filtered aggregation |
| Q6 | Revenue by Plan Tier | Multi-dimensional GROUP BY |
| Q7 | Revenue by Customer Segment | Multi-dimensional GROUP BY |
| Q8 | Revenue Growth MoM | Window function |
| Q9 | Revenue Growth YoY | Window function |
| Q10 | Rolling 3-Month Revenue Average | Window function |
| Q11 | Cumulative Revenue YTD | Window function |
| Q12 | Top-10% Customer Revenue Share | Ranking / window function |
| Q13 | Monthly Aggregated Revenue (anomaly input) | Time-series aggregation |

---

## Benchmarking Protocol

- 10 runs per query per volume level
- Cold run (run 1) and warm runs (runs 2–10) reported separately
- Mean ± standard deviation reported
- DuckDB: time.perf_counter() at Python level
- Snowflake: USE_CACHED_RESULT = FALSE enforced before every benchmark session
- All results saved as CSV to 05_benchmark_results/

---

## Evaluation Dimensions (5)

| # | Dimension | Primary Metric |
|---|---|---|
| 1 | Technical query performance | Execution time in ms by query type |
| 2 | Total cost / TCO | Snowflake credits vs. DuckDB personnel hours |
| 3 | Implementation effort / Time-to-Value | Hours until first working dashboard |
| 4 | Governance & scalability | RBAC effort, audit capability, concurrent users |
| 5 | AI compatibility | Share of AI-generated code, review effort, corrections |

---

## Repository Structure

```
01_scope/                 # architecture_overview.md, kpi_catalog.md, measurement_concept.md
02_data_generation/       # generator.py (chunked), config.yaml, validate_data.py
03_embedded_dwh/          # schema.sql, load_data.py, kpi_queries.sql, benchmark.py,
                          # anomaly_detection.py (to be added Phase 2C)
04_cloud_dwh/             # schema.sql, load_data.py, kpi_queries.sql, benchmark.py,
                          # anomaly_detection.py (to be added Phase 3C)
05_benchmark_results/     # benchmark_environment.md, result CSVs, plots
06_dbt/                   # dbt_project.yml, profiles.yml, models/ (Phase 2B + 3C)
```

---

## Current Status

| Phase | Description | Status |
|---|---|---|
| 0 | Local environment setup | ✅ Complete (14.04.2026) |
| 1 | Synthetic data generation (all 3 volumes) | ✅ Complete (14.04.2026) |
| 2A | DuckDB: schema + data loading (all 3 volumes) | ✅ Complete (14.04.2026) |
| 2B | DuckDB: dbt Core setup + models | ⏳ Pending |
| 2C | DuckDB: anomaly detection script | ⏳ Pending |
| 2D | Power BI: DuckDB ODBC dashboard | ⏳ Pending |
| 2E | DuckDB: benchmark.py full run | ⏳ Pending |
| 3 | Snowflake registration + implementation | ⏳ Pending (after Phase 2 complete) |
| 4 | Benchmarking (both platforms) | ⏳ Pending |
| 5 | Evaluation + AI tracking documentation | ⏳ Pending |

---

## Scope Rules (Non-negotiable)

1. Exactly two platforms: DuckDB and Snowflake. No others.
2. Identical SQL logic across platforms. Syntax adjustments only — no logical rewrites.
3. Identical dataset: same generator seed, same schema, same volume levels.
4. Identical anomaly detection implementation on both platforms.
5. No scope extensions after benchmarking starts.
6. BI tool: Power BI Desktop only. No Metabase, no Tableau, no other tools.
7. dbt: models must be structurally identical for DuckDB and Snowflake targets.

---

## Working Standards

- All benchmark scripts must accept --volume as argument.
- Result CSVs must include: query_label, n_runs, mean_ms, std_ms, min_ms, max_ms,
  cold_run_ms, warm_mean_ms.
- Generator is reproducible: fixed seed documented in config.yaml.
- Commit messages: type: description (e.g., feat: add dbt staging models for DuckDB)
- Never modify SQL logic between platforms — syntax only.
- Never add platforms, ML models, or analysis types beyond what is defined above.
- Ask before making structural changes to the repository.

---

## Local Environment

```
OS: Windows 11 Home, Version 25H2, x64
Device: Huawei MateBook D 15
CPU: Intel Core i3-1115G4, 2 cores / 4 threads, 3.00 GHz
RAM: 8.00 GB (7.80 GB usable)
Storage: PCIe SSD, 256 GB
Python: 3.11.8 (venv at .venv/)
DuckDB: 1.1.3
```