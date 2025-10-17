"""Airflow DAG definition for the shortage detection pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict

import pandas as pd
from airflow.decorators import dag, task

from src import analytics, ingest, report, shortage_logic, transform
from src.io import write_parquet
from src.quality import run_quality_checks
from src.utils import (
    RulesConfig,
    SettingsConfig,
    ensure_directories,
    load_rules,
    load_settings,
    project_root,
)


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _load_configs() -> Dict[str, object]:
    root = project_root()
    settings: SettingsConfig = load_settings(root / "config" / "settings.yaml")
    rules: RulesConfig = load_rules(root / "config" / "rules.yaml")
    ensure_directories(settings.output_processed_dir)
    return {"settings": settings, "rules": rules}


@dag(
    default_args=DEFAULT_ARGS,
    dag_id="shortage_pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Daily pipeline that ingests invoices, flags shortages, and publishes analytics.",
    tags=["shortage", "billing"],
)
def shortage_pipeline():
    """Define the Airflow tasks for the shortage pipeline."""

    @task()
    def ingest_data() -> str:
        configs = _load_configs()
        settings = configs["settings"]
        df = ingest.ingest_invoices(settings)
        staging_path = settings.output_processed_dir / "_staging_ingested.parquet"
        write_parquet(df, staging_path)
        return str(staging_path)

    @task()
    def transform_data(ingested_path: str) -> str:
        configs = _load_configs()
        settings = configs["settings"]
        df = pd.read_parquet(ingested_path)
        transformed = transform.transform_invoices(df, settings)
        report.export_clean_dataset(transformed, settings)
        staging_path = settings.output_processed_dir / "_staging_transformed.parquet"
        write_parquet(transformed, staging_path)
        return str(staging_path)

    @task()
    def apply_shortage_logic(transformed_path: str) -> str:
        configs = _load_configs()
        settings = configs["settings"]
        rules = configs["rules"]
        df = pd.read_parquet(transformed_path)
        flagged = shortage_logic.apply_shortage_logic(df, settings, rules)
        report.export_shortage_outputs(flagged, settings)
        flagged_path = settings.output_processed_dir / "_staging_flagged.parquet"
        write_parquet(flagged, flagged_path)
        return str(flagged_path)

    @task()
    def compute_analytics(flagged_path: str) -> Dict[str, str]:
        configs = _load_configs()
        settings = configs["settings"]
        df = pd.read_parquet(flagged_path)
        tables = analytics.compute_kpis(df, settings)
        exported = report.export_analytics_tables(tables, settings)
        return {key: str(path) for key, path in exported.items()}

    @task()
    def validate_data(flagged_path: str) -> None:
        configs = _load_configs()
        settings = configs["settings"]
        rules = configs["rules"]
        df = pd.read_parquet(flagged_path)
        run_quality_checks(df, settings, rules)

    ingested = ingest_data()
    transformed = transform_data(ingested)
    flagged = apply_shortage_logic(transformed)
    compute_analytics(flagged)
    validate_data(flagged)


dag_instance = shortage_pipeline()
