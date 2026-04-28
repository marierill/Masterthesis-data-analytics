# REPRODUCING.md – Reproducing the Benchmark

This document provides complete step-by-step instructions to reproduce
all benchmark results from scratch on a compatible system.

**Estimated time:** 3–5 hours (dominated by data generation and Snowflake loading)

---

## Goals

- Full reproducibility of all synthetic datasets (fixed seed)
- Identical benchmark workload on both platforms
- Verifiable results traceable to specific Git commits

## Limitations

- Snowflake measurements include network latency to AWS EU Frankfurt
  and cannot be exactly reproduced on different network conditions
- Absolute DuckDB runtimes depend on local hardware (see Section 1)
- Snowflake Free Trial required (30-day limit, ~$4 credit consumption)
- Windows-specific: ODBC driver installation steps apply to Windows only

---

## 1. System Requirements

| Component | Minimum | Used in this project |
|---|---|---|
| OS | Windows 10/11, Linux, macOS | Windows 11 Home x64 |
| CPU | Any x64, 2+ cores | Intel Core i3-1115G4, 2C/4T |
| RAM | 8 GB | 8 GB |
| Storage | 15 GB free | PCIe SSD, 256 GB |
| Python | 3.11.x | 3.11.8 |
| Internet | Required for Snowflake | Required |

**Note on RAM:** 8 GB is the minimum. The large-volume generator (20M rows)
uses chunked writing (2M rows/chunk) to stay within 8 GB. On systems with
less RAM, reduce `CHUNK_SIZE` in `02_data_generation/generator.py`.

---

## 2. Repository Setup

```bash
# Clone the repository
git clone https://github.com/marierill/Masterthesis-data-analytics.git
cd Masterthesis-data-analytics

# Create virtual environment (Python 3.11 required)
py -3.11 -m venv .venv

# Activate (Windows)
.\.venv\Scripts\Activate.ps1
# Activate (Linux/macOS)
source .venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Verify installation
pytest tests/test_sanity.py -v
# Expected: 1 passed
```

---

## 3. Synthetic Data Generation

```bash
# Generate all three volume levels
# small (~500k rows, ~2 min)
python 02_data_generation/generator.py --volume small
python 02_data_generation/validate_data.py --data-dir data/generated/small --expected-rows 500000

# medium (~5M rows, ~5 min)
python 02_data_generation/generator.py --volume medium
python 02_data_generation/validate_data.py --data-dir data/generated/medium --expected-rows 5000000

# large (~20M rows, ~15 min) – close all other applications first
python 02_data_generation/generator.py --volume large
python 02_data_generation/validate_data.py --data-dir data/generated/large --expected-rows 20000000
```

Expected output per volume: `Generated volume='...' rows=... at data\generated\...`
Expected validation: `Validation successful for data\generated\...`

**Reproducibility:** All datasets are fully reproducible via fixed seeds:
- small: seed 53 (base 42 + offset 11)
- medium: seed 59 (base 42 + offset 17)
- large: seed 65 (base 42 + offset 23)

---

## 4. DuckDB Implementation

### 4.1 Load Data

```bash
python 03_embedded_dwh/load_data.py \
  --data-dir data/generated/small \
  --db-path data/generated/small/benchmark.duckdb

python 03_embedded_dwh/load_data.py \
  --data-dir data/generated/medium \
  --db-path data/generated/medium/benchmark.duckdb

python 03_embedded_dwh/load_data.py \
  --data-dir data/generated/large \
  --db-path data/generated/large/benchmark.duckdb
```

Expected: `Done. Database: data/generated/{volume}/benchmark.duckdb`

### 4.2 dbt Transformation Layer

Install dbt adapter (already in requirements.txt):
```bash
pip install dbt-duckdb==1.10.1
```

Create `~/.dbt/profiles.yml`:
```yaml
masterthesis:
  target: duckdb
  outputs:
    duckdb:
      type: duckdb
      path: "<absolute-path-to-repo>/data/generated/small/benchmark.duckdb"
      threads: 4
    snowflake:
      type: snowflake
      account: "<your-account>"
      user: "<your-user>"
      password: "<your-password>"
      database: "BENCHMARK"
      schema: "MAIN"
      warehouse: "BENCHMARK_WH"
      threads: 4
```

```bash
dbt debug --project-dir 06_dbt\masterthesis --profiles-dir ~/.dbt
dbt run --project-dir 06_dbt\masterthesis --profiles-dir ~/.dbt
# Expected: PASS=3 WARN=0 ERROR=0
```

### 4.3 Run DuckDB Benchmark

```bash
# Run for all three volumes
python 03_embedded_dwh/benchmark.py \
  --db-path data/generated/small/benchmark.duckdb \
  --volume small --output-dir 05_benchmark_results

python 03_embedded_dwh/benchmark.py \
  --db-path data/generated/medium/benchmark.duckdb \
  --volume medium --output-dir 05_benchmark_results

python 03_embedded_dwh/benchmark.py \
  --db-path data/generated/large/benchmark.duckdb \
  --volume large --output-dir 05_benchmark_results
```

Expected: `Results written to: 05_benchmark_results/duckdb_{volume}_results.csv`

### 4.4 Run DuckDB Anomaly Detection

```bash
python 03_embedded_dwh/anomaly_detection.py \
  --db-path data/generated/small/benchmark.duckdb --volume small

python 03_embedded_dwh/anomaly_detection.py \
  --db-path data/generated/medium/benchmark.duckdb --volume medium

python 03_embedded_dwh/anomaly_detection.py \
  --db-path data/generated/large/benchmark.duckdb --volume large
```

Expected consensus anomaly: **February 2025** on all volume levels.

---

## 5. Snowflake Implementation

### 5.1 Prerequisites

1. Register Snowflake Free Trial at https://signup.snowflake.com
   - Edition: Standard
   - Cloud: AWS
   - Region: EU (Frankfurt)

2. In Snowflake (Snowsight SQL editor), run:
```sql
CREATE WAREHOUSE IF NOT EXISTS BENCHMARK_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS BENCHMARK;
CREATE SCHEMA IF NOT EXISTS BENCHMARK.MAIN;
USE DATABASE BENCHMARK;
USE SCHEMA MAIN;
```

3. Create `.env` file in repo root:
```
SNOWFLAKE_ACCOUNT=<your-account-identifier>
SNOWFLAKE_USER=<your-username>
SNOWFLAKE_PASSWORD=<your-password>
SNOWFLAKE_DATABASE=BENCHMARK
SNOWFLAKE_SCHEMA=MAIN
SNOWFLAKE_WAREHOUSE=BENCHMARK_WH
```

### 5.2 Load Data (one volume at a time)

**Important:** Snowflake uses a single shared database. Load and benchmark
each volume sequentially. The schema is recreated (DROP + CREATE) on each load.

```bash
# small
python 04_cloud_dwh/load_data.py \
  --data-dir data/generated/small \
  --schema-sql 04_cloud_dwh/schema.sql --env-file .env

python 04_cloud_dwh/benchmark.py --volume small --output-dir 05_benchmark_results
python 04_cloud_dwh/anomaly_detection.py --volume small

# medium
python 04_cloud_dwh/load_data.py \
  --data-dir data/generated/medium \
  --schema-sql 04_cloud_dwh/schema.sql --env-file .env

python 04_cloud_dwh/benchmark.py --volume medium --output-dir 05_benchmark_results
python 04_cloud_dwh/anomaly_detection.py --volume medium

# large
python 04_cloud_dwh/load_data.py \
  --data-dir data/generated/large \
  --schema-sql 04_cloud_dwh/schema.sql --env-file .env

python 04_cloud_dwh/benchmark.py --volume large --output-dir 05_benchmark_results
python 04_cloud_dwh/anomaly_detection.py --volume large
```

### 5.3 dbt Snowflake Target

```bash
dbt run --project-dir 06_dbt\masterthesis --profiles-dir ~/.dbt --target snowflake
# Expected: PASS=3 WARN=0 ERROR=0
```

---

## 6. Evaluation

```bash
python 05_benchmark_results/evaluation.py
```

Expected outputs in `05_benchmark_results/plots/`:
- `warm_mean_comparison.png`
- `scaling_behavior.png`
- `cold_warm_ratio.png`
- `crossover_analysis.png`
- `tco_comparison.png`

Expected crossover: Q5 ARPA at large volume (Snowflake faster than DuckDB).

---

## 7. Expected Results

| Query | DuckDB small (ms) | Snowflake small (ms) | Crossover at large? |
|---|---|---|---|
| Q1 Total Revenue | ~1 | ~175 | No |
| Q5 ARPA | ~37 | ~319 | **Yes** (SF: 572ms vs DK: 1030ms) |
| Q8 MoM Growth | ~9 | ~187 | No |
| Q13 Anomaly Input | ~6 | ~123 | No |

Anomaly detection consensus result: **February 2025** (all volumes, both platforms).

---

## 8. Troubleshooting

**OOM crash during large data generation:**
Reduce `CHUNK_SIZE` in `02_data_generation/generator.py` from 2,000,000 to 1,000,000.

**Snowflake DATE casting error:**
Already handled in `load_data.py` via VARIANT staging workaround.
If error persists, check that `USE_LOGICAL_TYPE = TRUE` is set in COPY INTO.

**DuckDB ODBC driver installation fails (Windows):**
Install Microsoft Visual C++ Redistributable 2022 x64 first.
If installer still fails, manually register DLLs via Windows Registry:
`HKLM:\SOFTWARE\ODBC\ODBCINST.INI\DuckDB Driver`

**dbt Snowflake: env_var() not found:**
Set environment variables before running dbt:
```powershell
$env:SNOWFLAKE_USER = "your-username"
$env:SNOWFLAKE_PASSWORD = "your-password"
```

**Snowflake IP access blocked:**
Remove network policy via Python connector:
```python
cur.execute('ALTER ACCOUNT UNSET NETWORK_POLICY')
```
