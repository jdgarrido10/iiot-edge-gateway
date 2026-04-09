"""
ETL transformation steps: column normalisation, cleaning, enrichment, and output preparation.
"""

import pandas as pd
import numpy as np

from . import etl_config, timestamps
from core.logger import setup_logger

logger = setup_logger(__name__)


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Rename source columns to canonical names using ``COLUMN_ALIASES``.

    Columns that already use a canonical name are left unchanged.
    Only the first matching alias per canonical name is applied.

    Args:
        df: Input DataFrame with raw source column names.

    Returns:
        DataFrame with canonical column names.
    """
    if df.empty:
        return df

    rename_map: dict[str, str] = {}
    existing = set(df.columns)

    for canonical, aliases in etl_config.COLUMN_ALIASES.items():
        if canonical in existing:
            continue
        for alias in aliases:
            if alias in existing:
                rename_map[alias] = canonical
                break

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def process_weights_and_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Apply minimal cleaning: timestamp validation and numeric weight coercion.

    Steps:
    1. Normalise column names.
    2. Parse ``timestamp`` to UTC-aware datetime; drop rows with null timestamps.
    3. Coerce ``net_weight`` to float; drop rows that cannot be parsed.

    Weight range filtering is intentionally omitted here — business rules
    that gate on weight thresholds belong in :func:`enrich_business_data`.

    Args:
        df: Input DataFrame after column normalisation.

    Returns:
        Cleaned DataFrame with invalid rows removed.
    """
    df = normalize_column_names(df)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df = df.dropna(subset=["timestamp"])

    if "net_weight" in df.columns:
        df["net_weight"] = pd.to_numeric(df["net_weight"], errors="coerce")
        df = df.dropna(subset=["net_weight"])

    return df


def enrich_business_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply business-logic enrichment for Grafana dashboards.

    Enrichment steps:
    1. Initialise ``tare_g`` to 0 if absent.
    2. Convert ``net_weight`` (kg) to grams: ``clean_net_weight_g = net_weight * 1000 - tare_g``.
    3. Derive ``quality_status`` from ``quality_status`` > ``reject_flag`` > default ``"OK"``.
    4. Calculate ``shift`` label from timestamp via :func:`timestamps.calculate_shift_vectorized`.
    5. Fill ``active_workers`` with 1 if absent or null.

    Args:
        df: DataFrame after :func:`process_weights_and_cleaning`.

    Returns:
        Enriched DataFrame.
    """
    if df.empty:
        return df

    # 1. Tare baseline
    if "tare_g" not in df.columns:
        df["tare_g"] = 0.0

    # 2. Weight conversion kg → g
    df["clean_net_weight_g"] = (df["net_weight"] * 1000.0) - df["tare_g"]

    # 3. Quality status
    if "quality_status" in df.columns:
        df["quality_status"] = (
            df["quality_status"].fillna("UNKNOWN").astype(str).str.strip().str.upper()
        )
    elif "reject_flag" in df.columns:
        df["reject_flag"] = pd.to_numeric(df["reject_flag"], errors="coerce").fillna(0)
        df["quality_status"] = np.where(df["reject_flag"] != 0, "REJECTED", "OK")
    else:
        df["quality_status"] = "OK"

    # 4. Shift label
    if "timestamp" in df.columns:
        df["shift"] = timestamps.calculate_shift_vectorized(df["timestamp"])
    else:
        df["shift"] = "Unknown"

    # 5. Worker count
    if "active_workers" not in df.columns:
        df["active_workers"] = 1
    else:
        df["active_workers"] = df["active_workers"].fillna(1).astype(int)

    return df


def prepare_influx_output(df: pd.DataFrame) -> pd.DataFrame:
    """Select and order columns for InfluxDB write.

    Missing metadata columns (``mqtt_topic``, ``batch_code``, ``linea``,
    ``batch_instance_id``) are filled with neutral sentinel values so that
    Influx tag cardinality remains predictable.

    Args:
        df: Fully enriched DataFrame.

    Returns:
        DataFrame containing only the columns defined in ``INFLUX_COLUMNS``.
    """
    defaults = {
        "mqtt_topic": "unknown",
        "batch_code": "no_batch",
        "linea": "unknown",
        "batch_instance_id": "unknown",
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default

    valid_cols = [c for c in etl_config.INFLUX_COLUMNS if c in df.columns]

    # Ensure these columns are always included when present, even if not in INFLUX_COLUMNS
    for extra_col in ("record_uuid", "batch_start_time"):
        if extra_col in df.columns and extra_col not in valid_cols:
            valid_cols.append(extra_col)

    return df[valid_cols].copy()


def prepare_minio_output(
    df_processed: pd.DataFrame, df_raw: pd.DataFrame
) -> pd.DataFrame:
    """Build the MinIO output by merging calculated fields onto the raw snapshot.

    When ``record_uuid`` is present in both DataFrames, calculated fields
    (``quality_status``, ``clean_net_weight_g``, ``tare_g``) are joined by
    UUID to preserve the raw payload intact. Otherwise the raw DataFrame is
    returned as-is.

    Args:
        df_processed: Fully enriched DataFrame (post ETL).
        df_raw: Raw snapshot taken before any destructive transformations.

    Returns:
        DataFrame suitable for CSV archival in MinIO.
    """
    if "record_uuid" in df_processed.columns and "record_uuid" in df_raw.columns:
        desired_cols = ["record_uuid", "quality_status", "clean_net_weight_g", "tare_g"]
        existing_cols = [c for c in desired_cols if c in df_processed.columns]
        cols_subset = df_processed[existing_cols].copy()

        cols_subset = cols_subset.rename(
            columns={
                "quality_status": "processed_status",
                "clean_net_weight_g": "calculated_net_weight",
            }
        )

        return df_raw.merge(cols_subset, on="record_uuid", how="left")

    return df_raw.copy()
