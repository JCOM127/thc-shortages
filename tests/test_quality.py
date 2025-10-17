"""Tests for quality validation."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.quality import run_quality_checks
from src.utils import RulesConfig, SettingsConfig


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


def _rules() -> RulesConfig:
    return RulesConfig(
        eligible_statuses=[
            "PAID",
            "PAID_PRICE_DISCREPANCY",
            "PROCESSING_PENDING_AMAZON_ACTION",
            "QUEUED_FOR_PAYMENT",
        ],
        shortage_required_flags=["Any Deductions", "Child_Invoice_Present"],
        use_strict_currency_check=True,
    )


def _valid_dataframe() -> pd.DataFrame:
    today = date.today()
    return pd.DataFrame(
        {
            "Invoice Date": [today],
            "Payment Due Date": [today - timedelta(days=5)],
            "Invoice Status": ["PAID"],
            "Actual Paid Amount": [95.0],
            "Paid Amount Currency": ["USD"],
            "Invoice Creation Date": [today - timedelta(days=10)],
            "Randomized Invoice": ["INV-001"],
            "Invoice Amount": [100.0],
            "Invoice Currency": ["USD"],
            "Any Deductions": [True],
            "Randomized PO": ["PO-123"],
            "Invoice_Delta": [5.0],
            "Child_Invoice_Present": [False],
            "Payment_Year": [today.year],
            "Shortage_Flag": [True],
            "Shortage_Amount_USD": [5.0],
            "Days_Past_Due": [5],
            "Age_Bucket": ["Current"],
        }
    )


def test_quality_checks_pass_for_valid_data() -> None:
    df = _valid_dataframe()
    run_quality_checks(df, _settings(), _rules())


def test_quality_checks_fail_for_future_dates() -> None:
    df = _valid_dataframe()
    df.loc[0, "Payment Due Date"] = date.today() + timedelta(days=1)
    with pytest.raises(ValueError):
        run_quality_checks(df, _settings(), _rules())
