"""Analytics aggregations for shortage detection."""

from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from .utils import SettingsConfig


LOGGER = logging.getLogger(__name__)


def compute_kpis(df: pd.DataFrame, settings: SettingsConfig) -> Dict[str, pd.DataFrame]:
    """Compute KPI tables from the shortage-evaluated DataFrame."""

    LOGGER.info("Computing analytics tables")
    shortage_df = df[df["Shortage_Flag"]].copy()

    total_shortage_amount = round(
        float(shortage_df["Shortage_Amount_USD"].sum()), settings.round_decimals
    )
    total_shortage = pd.DataFrame(
        {
            "Shortage_Count": [int(shortage_df.shape[0])],
            "Total_Shortage_USD": [total_shortage_amount],
        }
    )

    annual_shortages = (
        shortage_df.groupby("Payment_Year", dropna=True)
        .agg(
            Shortage_Count=("Shortage_Flag", "sum"),
            Total_Shortage_USD=("Shortage_Amount_USD", "sum"),
            Mean_Shortage_USD=("Shortage_Amount_USD", "mean"),
        )
        .reset_index()
    )
    if not annual_shortages.empty:
        annual_shortages["Shortage_Count"] = annual_shortages["Shortage_Count"].astype(int)
    for column in ("Total_Shortage_USD", "Mean_Shortage_USD"):
        annual_shortages[column] = annual_shortages[column].round(settings.round_decimals)

    aged_shortages = shortage_df[shortage_df["Age_Bucket"] == "Aged"].copy()
    aged_shortages_by_year = (
        aged_shortages.groupby("Payment_Year", dropna=True)
        .agg(
            Shortage_Count=("Shortage_Flag", "sum"),
            Total_Shortage_USD=("Shortage_Amount_USD", "sum"),
        )
        .reset_index()
    )
    if not aged_shortages_by_year.empty:
        aged_shortages_by_year["Shortage_Count"] = aged_shortages_by_year["Shortage_Count"].astype(int)
    aged_shortages_by_year["Total_Shortage_USD"] = aged_shortages_by_year[
        "Total_Shortage_USD"
    ].round(settings.round_decimals)

    aged_invoices = df[df["Age_Bucket"] == "Aged"].copy()
    aged_invoices_by_year = (
        aged_invoices.groupby("Payment_Year", dropna=True)
        .agg(
            Invoice_Count=("Randomized Invoice", "count"),
            Shortage_Count=("Shortage_Flag", "sum"),
            Total_Invoice_USD=("Invoice Amount", "sum"),
            Total_Shortage_USD=("Shortage_Amount_USD", "sum"),
        )
        .reset_index()
    )
    if not aged_invoices_by_year.empty:
        aged_invoices_by_year["Shortage_Count"] = aged_invoices_by_year["Shortage_Count"].astype(int)
        for column in ("Total_Invoice_USD", "Total_Shortage_USD"):
            aged_invoices_by_year[column] = aged_invoices_by_year[column].round(
                settings.round_decimals
            )

    LOGGER.info("Computed %d KPI tables", 4)

    return {
        "total_shortage": total_shortage,
        "annual_shortages": annual_shortages,
        "aged_shortages_by_year": aged_shortages_by_year,
        "aged_invoices_by_year": aged_invoices_by_year,
    }
