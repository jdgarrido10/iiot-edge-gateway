"""
ETL orchestrator: ingests Node-RED JSON payloads and produces InfluxDB and MinIO DataFrames.
"""

import pandas as pd
from typing import List, Dict, Tuple
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from . import common, processors
from core.logger import setup_logger

logger = setup_logger(__name__)

# Timezone of the physical factory location
_FACTORY_TZ = ZoneInfo("Europe/Madrid")


def process_node_red_payload(
    data_list: List[Dict],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Orchestrate the full ETL for a batch of Node-RED records.

    Processing order is critical:
    1. JSON normalisation and column aliasing.
    2. Timestamp parsing (HH:MM → UTC) and jitter application.
       Must happen *before* weight cleaning so that ``pd.to_datetime``
       coercion does not silently drop industrial weighing scales-format timestamps.
    3. Weight cleaning and numeric coercion.
    4. Business-logic enrichment.
    5. Output preparation for InfluxDB and MinIO.

    Args:
        data_list: List of raw JSON dicts received from Node-RED.

    Returns:
        Tuple of ``(df_influx, df_minio)``. Either DataFrame may be empty
        if all records are discarded during cleaning.
    """
    if not data_list:
        logger.warning("ETL received an empty record list")
        return pd.DataFrame(), pd.DataFrame()

    logger.info(f"ETL: processing {len(data_list)} records")

    df = pd.json_normalize(data_list)
    df = common.clean_json_keys(df)
    logger.debug(f"ETL: normalised {len(df)} records")

    # Raw snapshot for MinIO — taken before any destructive transformations
    df_minio_raw = df.copy()

    # Step 1: column aliasing
    df = processors.normalize_column_names(df)
    logger.debug("ETL: column names normalised")

    # Step 2: timestamp parsing + jitter (must precede weight cleaning)
    df = _normalize_timestamps_and_apply_jitter(df)

    if df.empty:
        logger.warning("ETL: all records discarded during timestamp normalisation")
        return pd.DataFrame(), df_minio_raw

    # Step 3: weight cleaning
    df = processors.process_weights_and_cleaning(df)

    if df.empty:
        logger.warning("ETL: all records discarded during weight cleaning")
        return pd.DataFrame(), df_minio_raw

    # Step 4: business enrichment
    df = processors.enrich_business_data(df)

    # Step 5a: InfluxDB output
    df_influx = processors.prepare_influx_output(df)

    # Step 5b: MinIO output — align column names then merge calculated fields
    if "record_uuid" in df.columns:
        df_minio_raw = processors.normalize_column_names(df_minio_raw)

    df_minio = processors.prepare_minio_output(df, df_minio_raw)

    # Sync timestamps from the processed DataFrame back to df_minio
    df_minio = _sync_minio_timestamps(df, df_minio)

    return df_influx, df_minio


def _sync_minio_timestamps(df: pd.DataFrame, df_minio: pd.DataFrame) -> pd.DataFrame:
    """Propagate processed timestamps to the MinIO DataFrame.

    Strategy A — join by UUID (preferred, order-independent).
    Strategy B — positional copy when both DataFrames have the same length.

    Args:
        df: Processed DataFrame with authoritative ``timestamp`` column.
        df_minio: MinIO DataFrame to update.

    Returns:
        ``df_minio`` with ``timestamp`` synchronised to ``df``.
    """
    try:
        if "record_uuid" in df.columns and "record_uuid" in df_minio.columns:
            ts_map = df.set_index("record_uuid")["timestamp"]
            df_minio["timestamp"] = (
                df_minio["record_uuid"].map(ts_map).fillna(df_minio["timestamp"])
            )
        elif len(df) == len(df_minio):
            df_minio["timestamp"] = df["timestamp"].values

        if "timestamp" in df_minio.columns:
            df_minio["timestamp"] = pd.to_datetime(df_minio["timestamp"], utc=True)

    except Exception as e:
        logger.warning(f"MinIO timestamp sync failed: {e}")

    return df_minio


def _normalize_timestamps_and_apply_jitter(df: pd.DataFrame) -> pd.DataFrame:
    """Parse Industrial weighing scales HH:MM timestamps and apply identity-based jitter.

    Industrial weighing scales emit timestamps as ``HH:MM`` strings. This function:
    1. Detects the HH:MM format.
    2. Combines it with today's date in the factory timezone.
    3. Converts to UTC.
    4. Applies a deterministic microsecond jitter derived from ``identity``
       to guarantee uniqueness for InfluxDB and preserve ordering for
       ProductionMonitor.

    This step **must** run before :func:`processors.process_weights_and_cleaning`
    because that function uses ``pd.to_datetime(errors='coerce')`` which would
    silently convert HH:MM strings to NaT.

    Args:
        df: DataFrame with a ``timestamp`` column (any format).

    Returns:
        DataFrame with ``timestamp`` as UTC-aware datetimes, deduplicated
        via jitter. Rows with unparseable timestamps are dropped.
    """
    if df.empty or "timestamp" not in df.columns:
        logger.debug(
            "Timestamp normalisation skipped: empty DataFrame or missing column"
        )
        return df

    now_factory = datetime.now(_FACTORY_TZ)
    today_date = now_factory.date()

    if "identity" not in df.columns:
        logger.warning("Column 'identity' not found — using row index as fallback")
        df["identity"] = range(len(df))

    df["identity"] = (
        pd.to_numeric(df["identity"], errors="coerce").fillna(0).astype(int)
    )

    def _parse_timestamp(val) -> datetime:
        """Parse a single timestamp value to a UTC-aware datetime."""
        if isinstance(val, (pd.Timestamp, datetime)):
            dt = val.to_pydatetime() if isinstance(val, pd.Timestamp) else val
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        try:
            val_str = str(val).strip()

            # Short timestamp format detection (HH:MM)
            if len(val_str) <= 5 and val_str.count(":") == 1:
                parts = val_str.split(":")
                h, m = int(parts[0]), int(parts[1])

                if not (0 <= h <= 23 and 0 <= m <= 59):
                    raise ValueError(f"Invalid time value: {h}:{m}")

                dt_naive = datetime(
                    today_date.year,
                    today_date.month,
                    today_date.day,
                    hour=h,
                    minute=m,
                    second=0,
                    microsecond=0,
                )
                return dt_naive.replace(tzinfo=_FACTORY_TZ).astimezone(timezone.utc)

            # Fallback: standard ISO / pandas parsing
            return pd.to_datetime(val, utc=True)

        except Exception as e:
            logger.error(f"Failed to parse timestamp '{val}': {e}")
            return datetime.now(timezone.utc)

    df["timestamp"] = df["timestamp"].apply(_parse_timestamp)

    null_count = df["timestamp"].isna().sum()
    if null_count > 0:
        logger.warning(f"{null_count} null timestamps found — dropping rows")
        df = df.dropna(subset=["timestamp"])

    if df.empty:
        return df

    # Apply deterministic jitter: offset in microseconds derived from identity
    base_times = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    offset_us = df["identity"] % 10_000_000
    df["timestamp"] = base_times + pd.to_timedelta(offset_us, unit="us")

    unique_count = df["timestamp"].nunique()
    total_count = len(df)

    if unique_count < total_count:
        logger.warning(
            f"{total_count - unique_count} duplicate timestamps remain after jitter"
        )
    else:
        logger.debug(f"All {unique_count} timestamps are unique after jitter")

    logger.info(
        f"Timestamp normalisation complete: {total_count} records | "
        f"unique: {unique_count} | "
        f"range: {df['timestamp'].min()} → {df['timestamp'].max()}"
    )

    return df
