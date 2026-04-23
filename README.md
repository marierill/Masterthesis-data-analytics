# Masterthesis-data-analytics
**Analytics Platform Architecture Comparison**  
Cloud Data Warehouse (Snowflake) vs. Local Embedded Analytics (DuckDB)

This repository contains a reproducible analytics engineering project to compare two BI/analytics platform architectures under identical workloads as part of a Master's thesis in Digital Business Management (Hochschule Albstadt-Sigmaringen).

---

## 1. Objective

Build a controlled benchmark and evaluation setup to answer:
- How do Cloud DWHs and DuckDB compare under typical **SaaS BI/Analytics workloads**?
- What are the trade-offs in **performance, cost, implementation effort, and governance**?
- Under which conditions is each architecture the better choice?

**Overarching Research Question:**  
Under which conditions do local analytics platforms like DuckDB represent a technically and economically viable alternative to Cloud Data Warehouses like Snowflake for modern BI and analytics workloads?

---

## 2. Use Case: SaaS Revenue & Customer Intelligence

**Scenario:** A fictitious B2B SaaS company (~500 customers, 3 product plans, 24 months history) seeks to professionalise its revenue reporting and identify churn risks early.

### Fact & Dimension Tables
| Table | Content | ~Rows (at 5M base) |
|---|---|---|
| `fact_subscriptions` | Monthly subscription transactions (MRR, plan, status, changes) | 500k |
| `fact_events` | User events (logins, feature usage, session duration) | 400k |
| `dim_customers` | Customer master (segment, region, acquisition channel) | 2k |
| `dim_products` | Plans, price tiers, features per plan | 10 |
| `dim_date` | Calendar dimension (24 months) | 730 |

### Analysis Layers
- **Layer A – Standard BI Reporting (SQL):** MRR/ARR dashboard, Churn Rate, NRR, cohort retention analysis, revenue by plan/region/segment
- **Layer B – Explorative Analysis (Python/Pandas):** Revenue distribution, CLV calculation, feature-usage-to-churn correlation
- **Layer C – Data Mining:** Anomaly detection on MRR trends (Isolation Forest) + Customer segmentation (k-Means on RFM features)
- **Layer D – AI-assisted Development:** Time logging with vs. without Claude – at least 3 comparable tasks per platform

---

## 3. Scope Freeze (Non-negotiable)

To ensure methodological comparability, the following scope is fixed:
- **Exactly two architectures:** Cloud DWH (Snowflake Free Trial) vs. DuckDB (local, embedded)
- **Identical synthetic dataset** for both architectures (same seed, same schema)
- **Identical KPI logic and SQL workloads** (logical equivalence required; only syntax adjustments permitted)
- **Identical data mining tasks:** Anomaly detection (Isolation Forest) + Customer segmentation (k-Means/RFM)
- **Three data volume levels:**
  - **0.5M rows** (baseline)
  - **5M rows** (realistic SaaS scale)
  - **20M rows** (stress test)
- **Benchmark execution:** Each query run **10 times**; cold vs. warm runs documented separately

Out of scope:
- Additional architectures or platforms
- Real-time / streaming setups
- Extended ML beyond anomaly detection and segmentation
- Scope extensions after benchmarking starts

---

## 4. Measurement Concept

### Evaluation Dimensions & Weights (for Decision Matrix)
| Priority | Dimension | Weight | Thesis Chapter |
|---|---|---|---|
| 1 | Economic Efficiency & Time to Value | 30% | Ch. 5.2 |
| 2 | Technical Functionality | 25% | Ch. 5.1 |
| 3 | Performance | 20% | Ch. 5.4 |
| 4 | Data Privacy & Lock-in | 15% | Ch. 5.5 |
| 5 | Maintainability & Governance | 10% | Ch. 5.3 |

### Key Metrics per Dimension
**Economic Efficiency:** Setup cost (EUR), recurring platform cost (EUR/month), setup effort (h), implementation effort (h), Time to First Insight (h), AI efficiency gain (h + Δ%)

**Technical Functionality:** SQL feature coverage (checklist), Python integration (ordinal), data mining support (ordinal), file format support (binary), BI tool connectivity (ordinal)

**Performance:** Query runtime simple aggregation (s, median of 10 runs), query runtime complex query (s), dataset load time CSV/Parquet (s), cold vs. warm start delta (s), memory usage during query (MB)

**Data Privacy & Lock-in:** Data location (categorical), GDPR compliance (ordinal), vendor lock-in risk (Likert 1–5), migration effort (h, estimated)

**Maintainability & Governance:** Git compatibility (ordinal), RBAC (binary + h), schema change effort (h), pipeline automation (ordinal), metadata management (ordinal)

---

## 4a. Organisation Profiles (Reference Scenarios)

Three constructed reference profiles run as a red thread from Ch. 1.2 through to the scenario-based application of the decision matrix in Ch. 6.3. Each profile corresponds to one data volume level.

| | Profil 1 – NovaSaaS | Profil 2 – ScaleUp GmbH | Profil 3 – AnalyticsHub |
|---|---|---|---|
| **Type** | B2B SaaS Startup | Growth-stage SaaS KMU | Internal Analytics Unit (SaaS Group) |
| **Size** | 12 employees, 1 analyst | 80–120 employees, 1 analyst + controlling | 500–1.500 group, 8–12 analytics team |
| **Customers** | 80–150 | 500–1.500 | Multiple product lines |
| **Volume Level** | 0.5M rows | 5M rows | 20M rows |
| **Budget (Analytics)** | 40–70k EUR/yr | 150–300k EUR/yr | 500k–1M EUR/yr |
| **Top Priority** | Time to Value | Technical Functionality | Maintainability & Governance |

### Dimension Weights per Profile (for Decision Matrix Ch. 6.3)

| Dimension | Base | P1 Startup | P2 KMU | P3 Konzern-Unit |
|---|---|---|---|---|
| Economic Efficiency & TtV | 30% | **40%** | 25% | 15% |
| Technical Functionality | 25% | 20% | **35%** | 25% |
| Performance | 20% | 15% | 15% | 20% |
| Data Privacy & Lock-in | 15% | 10% | 15% | **25%** |
| Maintainability & Governance | 10% | **5%** | 10% | **30%** |

---

## 5. Repository Structure

```
01_scope/
    architecture_overview.md
    kpi_catalog.md
    measurement_concept.md
    scope_rules.md

02_data_generation/
    generator.py              # Parametrised synthetic data generator
    config.yaml               # Volume levels, seed, schema config
    validate.py               # Data quality checks
    exports/                  # CSV + Parquet outputs (gitignored if large)

03_embedded_dwh/
    schema/                   # DuckDB DDL
    queries/                  # SQL KPI queries + drilldowns
    analysis/                 # Python workflows (Pandas, scikit-learn)
    benchmark/                # Benchmark scripts (multi-run)

04_cloud_dwh/
    schema/                   # Snowflake DDL
    queries/                  # Adapted SQL (logic unchanged)
    benchmark/                # Benchmark scripts

05_benchmark_results/
    raw/                      # Raw benchmark outputs (CSV)
    aggregated/               # Mean/std results per volume level
    plots/                    # Visualisations
    evaluation_notebook.ipynb # Analysis + decision matrix

06_ai_time_log/
    time_log.csv              # Task, platform, time_with_ai, time_without_ai, delta
```

---

## 6. Project Phases

| Phase | Description | Status |
|---|---|---|
| 0 | Business Case, KPI Catalogue & Measurement Concept | ✅ Complete |
| 1 | Theoretical Framework (Thesis Ch. 2) | 🔄 In Progress |
| 2 | Synthetic Data Generation | ⏳ Pending |
| 3 | DuckDB Implementation | ⏳ Pending |
| 4 | Snowflake Implementation | ⏳ Pending |
| 5 | Benchmarking & Evaluation | ⏳ Pending |
| 6 | Decision Model | ⏳ Pending |

---

## 7. Working Agreements

- No logical query changes between platforms (syntax adjustments only).
- All benchmark scripts are versioned and parametrised.
- Results stored as CSV/Parquet with metadata (timestamp, volume, platform, run_id).
- Scope drift is treated as a project defect.
- AI time log entries are made immediately after each task (not reconstructed).

---

## 8. AI Assistance Guidelines

AI support (Claude Code) is used for:
- Schema blueprints, synthetic data logic, SQL query writing/optimisation
- Benchmark scripting and documentation generation
- Layer D time logging support

AI is **not** used for:
- Interpreting final benchmark results as ground truth
- Replacing methodological decisions
- Creating scope extensions

**Claude Code must always start by reading `CLAUDE.md` and `01_scope/` before making any changes.**
