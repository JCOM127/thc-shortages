"""Tests for analytics aggregations."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from src.analytics import compute_kpis
from src.utils import SettingsConfig


def _settings() -> SettingsConfig:
    return SettingsConfig(
        input_raw_dir=Path("data/raw"),
        output_processed_dir=Path("data/processed"),
        date_format="dayfirst",
        aging_days_threshold=90,
        currency_expected="USD",
        round_decimals=2,
        partition_by_year=True,
        tolerance_small_delta_usd=0.01,
    )


def test_compute_kpis_produces_aged_invoice_summary() -> None:
    df = pd.DataFrame(
        {
            "Payment_Year": [2023, 2024],
            "Shortage_Flag": [True, False],
            "Shortage_Amount_USD": [10.0, 0.0],
            "Invoice Amount": [110.0, 120.0],
            "Randomized Invoice": ["INV-1", "INV-2"],
            "Age_Bucket": ["Aged", "Aged"],
        }
    )

    tables = compute_kpis(df, _settings())
    aged_invoices = tables["aged_invoices_by_year"]

    assert not aged_invoices.empty
    assert set(aged_invoices["Payment_Year"]) == {2023, 2024}
