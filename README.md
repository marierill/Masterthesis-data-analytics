# Masterthesis-data-analytics
Data Analytics Project for Customer Data with Python, SQL &amp; PBI 

# Analytics Platform Architecture Comparison  
**Cloud Data Warehouse (Snowflake) vs. Local Embedded Analytics (DuckDB)**

This repository contains a reproducible analytics engineering project to compare two BI/analytics platform architectures under identical workloads:  
- **Architecture A:** Cloud Data Warehouse (e.g., Snowflake or comparable managed DWH)  
- **Architecture B:** **DuckDB** (local, embedded, file-based, Python-integrated)

The project is part of a Master's thesis focusing on **technical and economic evaluation** and resulting in a **decision model** for typical BI/analytics use cases.

---

## 1. Objective
Build a controlled benchmark and evaluation setup to answer:
- How do Cloud DWHs and DuckDB compare under typical **Controlling/BI workloads**?
- What are the trade-offs in **performance, cost, implementation effort, and governance**?
- Under which conditions is each architecture the better choice?

---

## 2. Scope Freeze (Non-negotiable)
To ensure methodological comparability, the following scope is fixed:
- **Exactly two architectures** (Cloud DWH vs. DuckDB). No third platform.
- **Identical synthetic dataset** for both architectures.
- **Identical KPI logic and SQL workloads** (logical equivalence required).
- **Identical data mining task**: Python-based anomaly detection.
- **Identical data volumes**:
  - **0.5M rows**
  - **5M rows**
  - **20M rows**
- Use case setup:
  - **Fact table:** transactions
  - **Dimensions:** time, product, customer, region, cost center
  - **History:** 24 months

Out of scope:
- additional architectures
- real-time / streaming setups
- extended ML beyond anomaly detection
- scope extensions after benchmarking starts

---

## 3. Deliverables (What "Done" Looks Like)
This repository will contain:
- Parametrized **synthetic data generator** (CSV + Parquet exports)
- Reference **Star Schema** (SQL DDL)
- KPI query set (SQL) + drilldown queries
- Python workflow for anomaly detection
- Benchmark scripts (multi-run execution, mean/std)
- Results dataset + plots + evaluation notebook
- Decision matrix / decision model draft (weights + scenarios)

---

## 4. Repository Structure
- `01_scope/`  
  Project definition artifacts (architecture_overview.md, kpi_catalog.md, measurement_concept.md, rules)

- `02_data_generator/`  
  Python generator, configuration, validation checks, dataset exports (ignored if too large)

- `03_duckdb/`  
  DuckDB schema, SQL queries, Python analysis workflow, benchmarking scripts

- `04_cloud_dwh/`  
  Cloud DWH schema, adapted SQL syntax where required (logic unchanged), benchmarking scripts

- `05_benchmark_results/`  
  Raw benchmark outputs, aggregated results, plots, evaluation notebooks

---

## 5. Project Phases (Execution Plan)
1) **Blueprint & KPI Framework**  
2) **Synthetic Data Generation (parametrized + validated)**  
3) **DuckDB Implementation**  
4) **Cloud DWH Implementation**  
5) **Benchmarking & Evaluation**  
6) **Decision Model Preparation**

---

## 6. Measurement Concept 
Metrics captured per volume level and workload:
- Query runtime (multi-run, mean & std)
- Storage footprint
- Transformation effort
- Implementation effort
- Time-to-First-Insight

Benchmarking rules:
- Each query executed **10 times**
- Separate **cold vs. warm** runs where applicable
- All environment/config settings documented

---

## 7. Working Agreements (Standards)
- No logical query changes between platforms (only syntax/platform adjustments).
- All benchmark scripts are versioned.
- Results are stored in a consistent format (`CSV/Parquet`) with metadata.
- Scope drift is treated as a project defect.

---

## 8. Claude Code / AI Assistance Guidelines
AI support is used for:
- schema blueprints, synthetic data logic, SQL query writing/optimization
- benchmark scripting and documentation generation

AI is **not** used for:
- interpreting final results as "truth"
- replacing methodological decisions
- creating scope extensions

**Claude Code should always start by reading `01_scope/` and follow the rules defined there.**

---

## 9. Next Actions
- Finalize `01_scope/`:
  - `architecture_overview.md`
  - `kpi_catalog.md`
  - `measurement_concept.md`
- Then: local environment hardening (requirements.txt, packages)
- Then: build `02_data_generator/` (config-driven generator + validation)



