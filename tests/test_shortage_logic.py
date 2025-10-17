"""Tests for shortage logic application."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from src.shortage_logic import apply_shortage_logic
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


def test_shortage_flag_triggers_when_conditions_met() -> None:
    df = pd.DataFrame(
        {
            "Invoice_Delta": [5.0, 0.005],
            "Any Deductions": [True, False],
            "Child_Invoice_Present": [False, False],
            "Invoice Status": ["PAID", "PAID"],
            "Payment Due Date": [
                date.today() - timedelta(days=100),
                date.today() - timedelta(days=10),
            ],
        }
    )

    flagged = apply_shortage_logic(df, _settings(), _rules())

    assert bool(flagged.loc[0, "Shortage_Flag"]) is True
    assert flagged.loc[0, "Shortage_Amount_USD"] == 5.0
    assert flagged.loc[0, "Age_Bucket"] == "Aged"

    assert bool(flagged.loc[1, "Shortage_Flag"]) is False
    assert flagged.loc[1, "Shortage_Amount_USD"] == 0.0
