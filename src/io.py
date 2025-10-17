"""Input/output utilities for invoice data."""

from __future__ import annotations

import shutil
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from .utils import SettingsConfig, ensure_directories


LOGGER = logging.getLogger(__name__)

EXPECTED_COLUMNS: Tuple[str, ...] = (
    "Marketplace",
    "Invoice Date",
    "Payment Due Date",
    "Invoice Status",
    "Actual Paid Amount",
    "Paid Amount Currency",
    "Payee",
    "Invoice Creation Date",
    "Randomized Invoice",
    "Invoice Amount",
    "Invoice Currency",
    "Any Deductions",
    "Quantity Variance Amount",
    "Price Variance Amount",
    "Quick Pay Discount Amount",
    "Randomized Latest Child Invoice",
    "Randomized PO",
)

DATE_COLUMNS: Tuple[str, ...] = (
    "Invoice Date",
    "Payment Due Date",
    "Invoice Creation Date",
)

REQUIRED_COLUMNS: Tuple[str, ...] = (
    "Invoice Date",
    "Payment Due Date",
    "Invoice Status",
    "Actual Paid Amount",
    "Paid Amount Currency",
    "Invoice Creation Date",
    "Randomized Invoice",
    "Invoice Amount",
    "Invoice Currency",
    "Any Deductions",
    "Randomized PO",
)

FLOAT_COLUMNS: Tuple[str, ...] = (
    "Actual Paid Amount",
    "Invoice Amount",
    "Quantity Variance Amount",
    "Price Variance Amount",
    "Quick Pay Discount Amount",
)

STRING_COLUMNS: Tuple[str, ...] = (
    "Marketplace",
    "Invoice Status",
    "Paid Amount Currency",
    "Payee",
    "Randomized Invoice",
    "Invoice Currency",
    "Randomized Latest Child Invoice",
    "Randomized PO",
)

BOOL_COLUMNS: Tuple[str, ...] = ("Any Deductions",)


TRUE_VALUES = {"true", "t", "yes", "y", "1", "on"}
FALSE_VALUES = {"false", "f", "no", "n", "0", "off"}


def read_invoice_data(settings: SettingsConfig) -> pd.DataFrame:
    """Read and combine invoice CSV files from the configured raw directory."""

    source_dir = settings.input_raw_dir
    if not source_dir.exists():
        raise FileNotFoundError(f"Raw input directory not found: {source_dir}")

    csv_files = sorted(source_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {source_dir}")

    LOGGER.info("Found %d raw CSV files in %s", len(csv_files), source_dir)
    frames: List[pd.DataFrame] = []
    for file_path in csv_files:
        frames.append(_load_single_file(file_path, settings))

    combined = pd.concat(frames, ignore_index=True)
    LOGGER.info("Combined %d rows from %d files", combined.shape[0], len(frames))
    return combined


def write_partitioned_parquet(
    df: pd.DataFrame,
    output_path: Path,
    partition_col: str,
) -> Path:
    """Write a DataFrame to Parquet partitioned by ``partition_col``."""

    # Placeholder for future S3/object-storage integration; currently local filesystem only.
    ensure_directories(output_path.parent)
    if output_path.exists():
        if output_path.is_dir():
            shutil.rmtree(output_path)
        else:
            output_path.unlink()

    df.to_parquet(output_path, partition_cols=[partition_col], engine="pyarrow", index=False)
    LOGGER.info("Wrote Parquet dataset partitioned by %s to %s", partition_col, output_path)
    return output_path


def write_parquet(df: pd.DataFrame, output_path: Path) -> Path:
    """Write a DataFrame to a Parquet file."""

    ensure_directories(output_path.parent)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    LOGGER.info("Wrote Parquet file to %s", output_path)
    return output_path


def write_csv(df: pd.DataFrame, output_path: Path) -> Path:
    """Write a DataFrame to CSV."""

    ensure_directories(output_path.parent)
    df.to_csv(output_path, index=False)
    LOGGER.info("Wrote CSV file to %s", output_path)
    return output_path


def _load_single_file(file_path: Path, settings: SettingsConfig) -> pd.DataFrame:
    """Load and clean a single CSV file."""

    LOGGER.info("Reading CSV file %s", file_path)
    dtype_map = _dtype_mapping()
    dayfirst = settings.date_format.lower() in {"dayfirst", "dd/mm/yyyy"}
    df = pd.read_csv(
        file_path,
        dtype=dtype_map,
        parse_dates=list(DATE_COLUMNS),
        dayfirst=dayfirst,
        keep_default_na=True,
    )

    df.columns = [column.strip() for column in df.columns]
    _assert_required_columns(df.columns)
    _add_missing_optional_columns(df)

    for column in DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column], errors="coerce", dayfirst=dayfirst).dt.date

    for column in FLOAT_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").round(settings.round_decimals)

    for column in STRING_COLUMNS:
        if column in df.columns:
            df[column] = df[column].astype("string").str.strip()

    for column in BOOL_COLUMNS:
        df[column] = df[column].apply(_to_bool)

    df = _filter_currency(df, settings.currency_expected)
    if df.empty:
        LOGGER.warning(
            "All rows filtered out from %s due to currency checks (expected %s)",
            file_path.name,
            settings.currency_expected,
        )
    df["Source_File"] = file_path.name
    ordered_columns = list(EXPECTED_COLUMNS) + ["Source_File"]
    df = df[ordered_columns]
    return df


def _filter_currency(df: pd.DataFrame, expected_currency: str) -> pd.DataFrame:
    """Filter rows by currency and warn about non-compliant records."""

    mask_paid = df["Paid Amount Currency"].str.upper() == expected_currency.upper()
    mask_invoice = df["Invoice Currency"].str.upper() == expected_currency.upper()
    compliant_mask = mask_paid & mask_invoice
    non_compliant = df[~compliant_mask]
    if not non_compliant.empty:
        LOGGER.warning(
            "Skipping %d rows due to non-%s currency values",
            non_compliant.shape[0],
            expected_currency,
        )
    return df[compliant_mask].reset_index(drop=True)


def _dtype_mapping() -> Dict[str, str]:
    """Return the dtype mapping for CSV ingestion."""

    dtype_map: Dict[str, str] = {}
    for column in STRING_COLUMNS:
        dtype_map[column] = "string"
    for column in BOOL_COLUMNS:
        dtype_map[column] = "string"
    # Floats handled post-read to allow coercion.
    return dtype_map


def _assert_required_columns(columns: Iterable[str]) -> None:
    """Raise if required columns are missing."""

    missing = [column for column in REQUIRED_COLUMNS if column not in columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _add_missing_optional_columns(df: pd.DataFrame) -> None:
    """Add optional columns as NA to simplify downstream processing."""

    for column in EXPECTED_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA


def _to_bool(value: object) -> bool:
    """Convert a value to boolean."""

    if isinstance(value, bool):
        return value
    if pd.isna(value):
        raise ValueError("Encountered missing value in required boolean column.")
    normalized = str(value).strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ValueError(f"Cannot convert value '{value}' to boolean.")
