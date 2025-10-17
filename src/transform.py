"""Business transformations on cleaned invoice data."""

from __future__ import annotations

import logging

import pandas as pd

from .utils import SettingsConfig


LOGGER = logging.getLogger(__name__)


def transform_invoices(df: pd.DataFrame, settings: SettingsConfig) -> pd.DataFrame:
    """Apply core transformations required downstream."""

    LOGGER.info("Starting transformation step")
    working = df.copy()

    working["Invoice Amount"] = working["Invoice Amount"].fillna(0.0)
    working["Actual Paid Amount"] = working["Actual Paid Amount"].fillna(0.0)
    working["Invoice_Delta"] = (
        working["Invoice Amount"] - working["Actual Paid Amount"]
    ).round(settings.round_decimals)

    child_series = working["Randomized Latest Child Invoice"].astype("string")
    working["Child_Invoice_Present"] = (
        child_series.str.strip().fillna("").ne("").astype(bool)
    )

    payment_due = pd.to_datetime(
        working["Payment Due Date"],
        errors="coerce",
        dayfirst=settings.date_format.lower() in {"dayfirst", "dd/mm/yyyy"},
    )
    working["Payment_Year"] = payment_due.dt.year.astype("Int64")

    LOGGER.info("Completed transformation")
    return working
