# Data Generation

Synthetic data generator for the benchmark star schema.

## Files
- `config.yaml`: generation parameters (seed, distributions, volume levels, export settings)
- `generator.py`: creates dimensions + `fact_billing_lines` and writes files + metadata
- `validate_data.py`: validates row count, FK integrity and value constraints

## Run
```powershell
python 02_data_generation/generator.py --volume small
python 02_data_generation/validate_data.py --data-dir data/generated/small --expected-rows 500000
```

Optional row override for smoke tests:
```powershell
python 02_data_generation/generator.py --volume small --rows 10000
```
