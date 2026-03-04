# Benchmark Environment Documentation

This document defines the technical environment under which all benchmarking measurements were executed.

Its purpose is to ensure:

- Reproducibility
- Transparency
- Methodological validity
- Separation of architecture effects from hardware effects

All values must be filled in before final benchmark evaluation.

---

# 1. Local System Environment (DuckDB)

## 1.1 Operating System

- OS Name:
- OS Version:
- Architecture (x64 / ARM):

## 1.2 Hardware Specifications

- CPU Model:
- Number of Physical Cores:
- Number of Logical Threads:
- Base Frequency:
- RAM (GB):
- Storage Type (SSD / NVMe / HDD):
- Available Disk Space at Benchmark Start:

## 1.3 Software Stack

- Python Version:
- DuckDB Version:
- Pandas Version:
- PyArrow Version:
- Matplotlib Version:
- Virtual Environment Tool (venv / conda / other):

## 1.4 DuckDB Configuration

- In-memory or file-based mode:
- Threads configured:
- PRAGMA settings (if modified):
- Temporary directory location:

---

# 2. Cloud Data Warehouse Environment

## 2.1 Provider Information

- Provider Name:
- Region:
- Warehouse Size / Compute Tier:
- Auto-scaling Enabled (Yes/No):
- Multi-cluster Enabled (Yes/No):

## 2.2 Storage Configuration

- Storage Model (separate / unified):
- Compression Enabled:
- Data Format (internal columnar / parquet ingestion / etc.):

## 2.3 Session & Performance Settings

- Result Caching Enabled (Yes/No):
- Query Acceleration Services Enabled (Yes/No):
- Concurrency Level During Tests:
- Manual Scaling Performed (Yes/No):

---

# 3. Dataset Configuration

For each benchmark round:

## 3.1 Dataset Volume

- 500k rows
- 5M rows
- 20M rows

(Indicate which volume was active during each run.)

## 3.2 File Format

- CSV / Parquet:
- Compression Type:
- Partitioning Strategy (if used):
- Data Generation Seed:

---

# 4. Benchmark Execution Configuration

## 4.1 Query Execution Rules

- Number of runs per query:
- Cold run reset method:
- Warm run definition:
- Query timeout settings:
- Parallel query execution allowed (Yes/No):

## 4.2 Measurement Method

- Timing method (Python timer / database profiling / system metrics):
- Time unit (ms / seconds):
- Overhead included/excluded:

## 4.3 Logging

- Log file location:
- Timestamp format:
- Result storage format (CSV / Parquet):

---

# 5. Time-to-First-Insight Measurement

For each architecture:

- Environment setup start timestamp:
- Schema deployment completion:
- Data load completion:
- First successful KPI execution:
- Total duration (calculated):

---

# 6. Known Constraints During Measurement

Document any relevant conditions:

- Background processes running:
- Cloud throttling events:
- Network latency issues:
- Hardware thermal throttling:
- Manual intervention during tests:

---

# 7. Version Control Reference

- Git Commit Hash:
- Branch Name:
- Date of Measurement:

All benchmark results must be traceable to a specific repository state.

---

# 8. Integrity Declaration

All benchmarks were executed:

- Without logical query changes between architectures
- Without selective optimization benefiting only one platform
- Without changing dataset distributions mid-benchmark

Any deviations must be documented explicitly.