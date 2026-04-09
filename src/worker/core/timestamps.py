"""
Timestamp utilities: shift calculation, normalisation, and timezone conversion.
"""

import pandas as pd
import numpy as np

from core.logger import setup_logger

logger = setup_logger(__name__)


def calculate_shift_vectorized(timestamp_series: pd.Series) -> pd.Series:
    """Map UTC timestamps to factory shift labels using Madrid local time.

    Shift boundaries:
    - **Mañana**: 06:00 – 13:59 (local)
    - **Tarde**: everything else

    Args:
        timestamp_series: UTC-aware datetime Series.

    Returns:
        String Series with values ``"Manana"`` or ``"Tarde"``.
    """
    try:
        from zoneinfo import ZoneInfo

        hour = timestamp_series.dt.tz_convert(ZoneInfo("Europe/Madrid")).dt.hour
    except Exception:
        hour = timestamp_series.dt.hour

    return np.where((hour >= 6) & (hour < 14), "Manana", "Tarde")


def normalize_timestamps(df: pd.DataFrame, col_name: str = "timestamp") -> pd.DataFrame:
    """Ensure a datetime column is UTC-aware, filling invalid values with *now*.

    If the column is absent entirely, a synthetic sequence spaced 10 ms apart
    ending at the current UTC time is generated as a fallback.

    Args:
        df: Input DataFrame.
        col_name: Name of the timestamp column to process.

    Returns:
        DataFrame with *col_name* as a UTC-aware datetime Series.
    """
    current_utc = pd.Timestamp.now(tz="UTC")

    if col_name in df.columns:
        df[col_name] = pd.to_datetime(df[col_name], errors="coerce")

        invalid_count = df[col_name].isna().sum()
        if invalid_count > 0:
            logger.warning(
                f"{invalid_count} invalid timestamps found — filling with current UTC",
                extra={"invalid_count": invalid_count, "column": col_name},
            )

        if df[col_name].dt.tz is None:
            df[col_name] = df[col_name].dt.tz_localize("UTC")
        else:
            df[col_name] = df[col_name].dt.tz_convert("UTC")

        df[col_name] = df[col_name].fillna(current_utc)

    else:
        logger.warning(
            f"Column '{col_name}' not found — generating synthetic timestamps (10 ms interval)",
            extra={"row_count": len(df)},
        )
        df[col_name] = pd.date_range(
            end=current_utc,
            periods=len(df),
            freq="10ms",
            tz="UTC",
        )

    return df


def convert_to_display_timezone(
    df: pd.DataFrame,
    col_name: str,
    target_tz: str = "Europe/Madrid",
) -> pd.DataFrame:
    """Convert a UTC datetime column to a local timezone for display/export.

    Timezone info is stripped after conversion so the result is compatible
    with naive-datetime consumers such as Excel exports.

    Args:
        df: Input DataFrame.
        col_name: Name of the UTC datetime column.
        target_tz: IANA timezone string for the target timezone.

    Returns:
        DataFrame with *col_name* as a timezone-naive local datetime Series.
    """
    try:
        utc_series = pd.to_datetime(df[col_name], utc=True)
        local_series = utc_series.dt.tz_convert(target_tz)
        df[col_name] = local_series.dt.tz_localize(None)
        logger.debug(f"Column '{col_name}' converted to {target_tz}")
    except Exception as e:
        logger.error(
            f"Timezone conversion failed for column '{col_name}'",
            extra={"error": str(e)},
            exc_info=True,
        )

    return df
