"""Shortage flag evaluation."""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from .utils import RulesConfig, SettingsConfig


LOGGER = logging.getLogger(__name__)


def apply_shortage_logic(
    df: pd.DataFrame,
    settings: SettingsConfig,
    rules: RulesConfig,
) -> pd.DataFrame:
    """Flag invoice shortages based on configured tolerance and rules."""

    LOGGER.info("Applying shortage logic")
    working = df.copy()

    tolerance = settings.tolerance_small_delta_usd
    normalized_status = working["Invoice Status"].astype("string").str.upper()
    allowed_statuses = set(rules.eligible_statuses)

    shortage_condition = (
        (working["Invoice_Delta"] > tolerance)
        & (
            working["Any Deductions"].astype(bool)
            | working["Child_Invoice_Present"].astype(bool)
        )
        & normalized_status.isin(allowed_statuses)
    )

    working["Shortage_Flag"] = shortage_condition
    working["Shortage_Amount_USD"] = working["Invoice_Delta"].where(shortage_condition, 0.0)

    payment_due = pd.to_datetime(working["Payment Due Date"], dayfirst=True, errors="coerce")
    today_value = pd.Timestamp(date.today())
    working["Days_Past_Due"] = (today_value - payment_due).dt.days
    working["Days_Past_Due"] = working["Days_Past_Due"].clip(lower=0).fillna(0).astype(int)

    working["Age_Bucket"] = working["Days_Past_Due"].apply(
        lambda days: "Aged" if days > settings.aging_days_threshold else "Current"
    )

    LOGGER.info(
        "Shortage logic flagged %d records",
        working["Shortage_Flag"].sum(),
    )
    return working
