"""Quality validation using Pandera."""

from __future__ import annotations

import logging
from datetime import date

import pandera as pa
from pandera import Check
import pandas as pd

from .utils import RulesConfig, SettingsConfig


LOGGER = logging.getLogger(__name__)


def run_quality_checks(df: pd.DataFrame, settings: SettingsConfig, rules: RulesConfig) -> None:
    """Execute quality assertions on the enriched DataFrame."""

    LOGGER.info("Running quality checks")
    LOGGER.debug("Eligible statuses for shortage logic: %s", rules.eligible_statuses)
    if df.empty:
        raise ValueError("Quality check failed: dataframe is empty.")

    schema = _build_schema(settings)
    schema.validate(df, lazy=True)

    _validate_currency(df, settings.currency_expected)
    _validate_dates(df)
    LOGGER.info("Quality checks passed")


def _build_schema(settings: SettingsConfig) -> pa.DataFrameSchema:
    """Construct the Pandera schema for quality enforcement."""

    return pa.DataFrameSchema(
        {
            "Invoice Date": pa.Column(pa.Object, nullable=False),
            "Payment Due Date": pa.Column(pa.Object, nullable=False),
            "Invoice Status": pa.Column(pa.String, nullable=False),
            "Actual Paid Amount": pa.Column(pa.Float, nullable=False, checks=Check.ge(0)),
            "Paid Amount Currency": pa.Column(pa.String, nullable=False),
            "Invoice Creation Date": pa.Column(pa.Object, nullable=False),
            "Randomized Invoice": pa.Column(pa.String, nullable=False),
            "Invoice Amount": pa.Column(pa.Float, nullable=False, checks=Check.ge(0)),
            "Invoice Currency": pa.Column(pa.String, nullable=False),
            "Any Deductions": pa.Column(pa.Bool, nullable=False),
            "Randomized PO": pa.Column(pa.String, nullable=False),
            "Invoice_Delta": pa.Column(pa.Float, nullable=False),
            "Child_Invoice_Present": pa.Column(pa.Bool, nullable=False),
            "Payment_Year": pa.Column(pa.Int64, nullable=True),
            "Shortage_Flag": pa.Column(pa.Bool, nullable=False),
            "Shortage_Amount_USD": pa.Column(pa.Float, nullable=False),
            "Days_Past_Due": pa.Column(pa.Int64, nullable=False, checks=Check.ge(0)),
            "Age_Bucket": pa.Column(pa.String, nullable=False),
        },
        strict=False,
        name="ShortageDataFrame",
    )


def _validate_currency(df: pd.DataFrame, expected_currency: str) -> None:
    """Ensure all currency fields match the configured currency."""

    expected_upper = expected_currency.upper()
    paid_valid = df["Paid Amount Currency"].str.upper() == expected_upper
    invoice_valid = df["Invoice Currency"].str.upper() == expected_upper
    if not (paid_valid & invoice_valid).all():
        raise ValueError("Non-compliant currency detected during quality checks.")


def _validate_dates(df: pd.DataFrame) -> None:
    """Validate date columns are not in the future."""

    today_value = date.today()
    for column in ("Invoice Date", "Payment Due Date", "Invoice Creation Date"):
        parsed = pd.to_datetime(df[column], errors="coerce")
        if parsed.isna().any():
            raise ValueError(f"Invalid date values found in column {column}")
        if (parsed.dt.date > today_value).any():
            raise ValueError(f"Future-dated values found in column {column}")
