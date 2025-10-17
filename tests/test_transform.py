"""Tests for src.transform."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from src.transform import transform_invoices
from src.utils import SettingsConfig


def _base_settings() -> SettingsConfig:
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


def test_transform_adds_expected_columns() -> None:
    df = pd.DataFrame(
        {
            "Invoice Amount": [105.0],
            "Actual Paid Amount": [100.0],
            "Randomized Latest Child Invoice": ["CHILD-123"],
            "Payment Due Date": [date(2024, 6, 1)],
        }
    )

    transformed = transform_invoices(df, _base_settings())

    assert "Invoice_Delta" in transformed.columns
    assert transformed.loc[0, "Invoice_Delta"] == 5.0
    assert bool(transformed.loc[0, "Child_Invoice_Present"]) is True
    assert transformed.loc[0, "Payment_Year"] == 2024
