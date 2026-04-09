"""
Test suite for timestamp formatting, shift assignments, and timezone conversions.
"""

import pandas as pd
from datetime import datetime, timezone

from core.timestamps import (
    calculate_shift_vectorized,
    normalize_timestamps,
    convert_to_display_timezone,
)


def test_calculate_shift_morning():
    """Verify that 08:00 AM Madrid local time resolves to Morning shift."""
    ts = pd.Series([datetime(2024, 1, 1, 7, 0, tzinfo=timezone.utc)])
    shifts = calculate_shift_vectorized(ts)
    assert shifts[0] == "Manana"


def test_calculate_shift_afternoon():
    """Verify that 15:00 PM Madrid local time resolves to Afternoon shift."""
    ts = pd.Series([datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc)])
    shifts = calculate_shift_vectorized(ts)
    assert shifts[0] == "Tarde"


def test_normalize_timestamps_missing_column():
    """Ensure a synthetic timestamp sequence is generated if the column is missing."""
    df = pd.DataFrame({"data": [1, 2, 3]})
    df_norm = normalize_timestamps(df, col_name="timestamp")

    assert "timestamp" in df_norm.columns
    assert df_norm["timestamp"].dt.tz == timezone.utc
    assert len(df_norm["timestamp"].unique()) == 3


def test_normalize_timestamps_invalid_values():
    """Ensure garbage timestamp data defaults safely to current UTC time."""
    df = pd.DataFrame({"timestamp": ["invalid_date_string", None]})
    df_norm = normalize_timestamps(df, col_name="timestamp")

    assert df_norm["timestamp"].isna().sum() == 0
    assert df_norm["timestamp"].dt.tz == timezone.utc


def test_calculate_shift_boundaries():
    """Validate strict hour boundaries for shift transitions."""
    ts = pd.Series(
        [
            datetime(2024, 1, 1, 5, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc),
        ]
    )
    shifts = calculate_shift_vectorized(ts)
    assert shifts[0] == "Manana"
    assert shifts[1] == "Tarde"


def test_convert_to_display_timezone():
    """Verify conversion to naive local time for flat file exports (e.g., CSV)."""
    df = pd.DataFrame({"time": [datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)]})
    df_local = convert_to_display_timezone(
        df, col_name="time", target_tz="Europe/Madrid"
    )

    assert df_local["time"].iloc[0].hour == 13
    assert df_local["time"].dt.tz is None
