"""Utility helpers for the shortage detection pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SettingsConfig:
    """Typed representation of values stored in ``config/settings.yaml``."""

    input_raw_dir: Path
    output_processed_dir: Path
    date_format: str
    aging_days_threshold: int
    currency_expected: str
    round_decimals: int
    partition_by_year: bool
    tolerance_small_delta_usd: float


@dataclass(frozen=True)
class RulesConfig:
    """Typed representation of values stored in ``config/rules.yaml``."""

    eligible_statuses: List[str]
    shortage_required_flags: List[str]
    use_strict_currency_check: bool


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging with a structured format once per process."""

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def load_yaml(path: Path) -> Dict[str, Any]:
    """Load a YAML file into a dictionary."""

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    LOGGER.debug("Loaded YAML configuration from %s", path)
    if not isinstance(data, dict):
        raise ValueError(f"Unexpected YAML structure in {path}")
    return data


def load_settings(path: Path) -> SettingsConfig:
    """Parse the settings configuration file into ``SettingsConfig``."""

    raw = load_yaml(path)
    settings = SettingsConfig(
        input_raw_dir=_resolve_path(path.parent.parent, raw["input_raw_dir"]),
        output_processed_dir=_resolve_path(path.parent.parent, raw["output_processed_dir"]),
        date_format=str(raw["date_format"]),
        aging_days_threshold=int(raw["aging_days_threshold"]),
        currency_expected=str(raw["currency_expected"]),
        round_decimals=int(raw["round_decimals"]),
        partition_by_year=bool(raw["partition_by_year"]),
        tolerance_small_delta_usd=float(raw["tolerance_small_delta_usd"]),
    )
    LOGGER.debug("Parsed settings: %s", settings)
    return settings


def load_rules(path: Path) -> RulesConfig:
    """Parse the rules configuration file into ``RulesConfig``."""

    raw = load_yaml(path)
    rules = RulesConfig(
        eligible_statuses=[str(status).upper() for status in raw["eligible_statuses"]],
        shortage_required_flags=[str(flag) for flag in raw["shortage_required_flags"]],
        use_strict_currency_check=bool(raw["use_strict_currency_check"]),
    )
    LOGGER.debug("Parsed rules: %s", rules)
    return rules


def ensure_directories(*paths: Path) -> None:
    """Create directories if they do not exist."""

    for path in paths:
        if path is None:
            continue
        resolved_path = Path(path)
        resolved_path.mkdir(parents=True, exist_ok=True)
        LOGGER.debug("Ensured directory exists: %s", resolved_path)


def _resolve_path(base: Path, relative: str) -> Path:
    """Resolve a repository-relative path."""

    candidate = Path(relative)
    if candidate.is_absolute():
        return candidate
    return (base / candidate).resolve()


def project_root() -> Path:
    """Return the repository root (two levels up from this file)."""

    return Path(__file__).resolve().parent.parent
