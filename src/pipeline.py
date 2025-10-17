"""End-to-end pipeline entrypoint."""

from __future__ import annotations

import logging
from typing import Dict

from . import analytics, ingest, report, shortage_logic, transform
from .quality import run_quality_checks
from .utils import (
    RulesConfig,
    SettingsConfig,
    configure_logging,
    ensure_directories,
    load_rules,
    load_settings,
    project_root,
)

LOGGER = logging.getLogger(__name__)


def run_pipeline() -> Dict[str, str]:
    """Execute the full shortage detection workflow."""

    configure_logging()
    root = project_root()
    settings: SettingsConfig = load_settings(root / "config" / "settings.yaml")
    rules: RulesConfig = load_rules(root / "config" / "rules.yaml")

    ensure_directories(settings.output_processed_dir)

    LOGGER.info("Pipeline started")
    invoices_raw = ingest.ingest_invoices(settings)
    if invoices_raw.empty:
        raise ValueError("No invoice data available after ingestion.")
    invoices_transformed = transform.transform_invoices(invoices_raw, settings)
    invoices_flagged = shortage_logic.apply_shortage_logic(invoices_transformed, settings, rules)

    run_quality_checks(invoices_flagged, settings, rules)

    clean_path = report.export_clean_dataset(invoices_transformed, settings)
    shortage_paths = report.export_shortage_outputs(invoices_flagged, settings)
    analytics_tables = analytics.compute_kpis(invoices_flagged, settings)
    analytics_paths = report.export_analytics_tables(analytics_tables, settings)

    LOGGER.info("Pipeline completed successfully")
    return {
        "clean_dataset": str(clean_path),
        **{key: str(path) for key, path in shortage_paths.items()},
        **{key: str(path) for key, path in analytics_paths.items()},
    }


if __name__ == "__main__":
    run_pipeline()
