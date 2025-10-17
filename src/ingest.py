"""Data ingestion orchestration."""

from __future__ import annotations

import logging
from typing import Tuple

import pandas as pd

from . import io
from .utils import SettingsConfig


LOGGER = logging.getLogger(__name__)


def ingest_invoices(settings: SettingsConfig) -> pd.DataFrame:
    """Load invoice data from configured CSV sources."""

    LOGGER.info("Starting ingestion step")
    df = io.read_invoice_data(settings)
    LOGGER.info("Completed ingestion with %d records", df.shape[0])
    return df
