"""Export utilities for pipeline outputs."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import pandas as pd

from . import io
from .utils import SettingsConfig


LOGGER = logging.getLogger(__name__)


def export_clean_dataset(df: pd.DataFrame, settings: SettingsConfig) -> Path:
    """Write the clean invoices dataset partitioned by payment year."""

    target = settings.output_processed_dir / "invoices_clean.parquet"
    LOGGER.info("Exporting clean dataset to %s", target)
    if settings.partition_by_year:
        return io.write_partitioned_parquet(df, target, partition_col="Payment_Year")
    return io.write_parquet(df, target)


def export_shortage_outputs(df: pd.DataFrame, settings: SettingsConfig) -> Dict[str, Path]:
    """Write shortage evaluation outputs."""

    flagged_path = settings.output_processed_dir / "shortages_flagged.parquet"
    shortages_only = settings.output_processed_dir / "shortages_only.parquet"

    io.write_parquet(df, flagged_path)
    shortage_records = df[df["Shortage_Flag"]]
    io.write_parquet(shortage_records, shortages_only)

    return {
        "shortages_flagged": flagged_path,
        "shortages_only": shortages_only,
    }


def export_analytics_tables(tables: Dict[str, pd.DataFrame], settings: SettingsConfig) -> Dict[str, Path]:
    """Write analytics tables to CSV files."""

    output_paths: Dict[str, Path] = {}
    mapping = {
        "total_shortage": "total_shortage.csv",
        "annual_shortages": "annual_shortages.csv",
        "aged_shortages_by_year": "aged_shortages_by_year.csv",
        "aged_invoices_by_year": "aged_invoices_by_year.csv",
    }
    for key, filename in mapping.items():
        if key not in tables:
            LOGGER.warning("Analytics table %s not provided; skipping export", key)
            continue
        path = settings.output_processed_dir / filename
        io.write_csv(tables[key], path)
        output_paths[key] = path
    return output_paths
