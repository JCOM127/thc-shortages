# THC Shortages Detection

This repository provides a configurable, production-ready data pipeline for identifying invoice shortages in Amazon supplier billing data. The solution ingests CSV files, standardises schemas, evaluates shortage logic, generates KPI summaries, and produces clean Parquet outputs suitable for downstream analytics or Airflow orchestration.

## Installation
- Ensure Python 3.13 is available.
- Create and activate a virtual environment.
- Install dependencies: `pip install -r requirements.txt`

## Local Usage
- Place raw invoice/payment CSVs into `data/raw`.
- Update configuration in `config/settings.yaml` and `config/rules.yaml` as needed.
- Run the end-to-end pipeline: `python -m src.pipeline`
- Outputs (Parquet, CSV summaries, logs) are written under `data/processed`.

## Configuration
- `config/settings.yaml` controls directories, date formats, rounding, partitioning, and threshold parameters.
- `config/rules.yaml` defines shortage eligibility statuses and flag requirements.
- All parameters are documented inline within the YAML files for quick reference.

## Outputs
- `data/processed/invoices_clean.parquet`: Cleaned invoices partitioned by `Payment_Year`.
- `data/processed/shortages_flagged.parquet`: All invoices with shortage flags.
- `data/processed/shortages_only.parquet`: Shortage-only subset.
- `data/processed/total_shortage.csv`, `annual_shortages.csv`, `aged_shortages_by_year.csv`, `aged_invoices_by_year.csv`: Aggregated KPI summaries.

## Airflow Deployment
- The DAG in `dags/shortage_pipeline_dag.py` orchestrates daily ingestion, transformation, shortage detection, analytics, and quality checks.
- Update Airflow connections/variables as needed for your environment. (Placeholders for cloud integrations are provided but disabled by default.)

## Future Enhancements
- Integrate object storage (e.g., S3) using the placeholders in `src/io.py`.
- Extend analytics outputs to BI dashboards.
- Containerise the pipeline for deployment across environments.
