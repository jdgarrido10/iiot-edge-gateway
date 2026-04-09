"""
Test suite for shared utilities: vectorised data cleaning and normalisation.
"""

import pandas as pd
import numpy as np

from core.common import clean_numeric_vectorized, clean_json_keys

# =====================================================================
# Numeric Coercion Tests
# =====================================================================


def test_clean_numeric_vectorized_handles_european_decimals():
    """Verify that European comma-delimited numbers are safely converted to floats."""
    raw_series = pd.Series(["500,5", "1000,0", "0,75"])

    result = clean_numeric_vectorized(raw_series)

    assert result.iloc[0] == 500.5
    assert result.iloc[1] == 1000.0
    assert result.iloc[2] == 0.75


def test_clean_numeric_vectorized_coerces_errors_to_nan():
    """Verify that malformed strings do not crash Pandas, but cast safely to NaN."""
    raw_series = pd.Series(["500.5", "sensor_error", "400", None])

    result = clean_numeric_vectorized(raw_series)

    assert result.iloc[0] == 500.5
    assert np.isnan(result.iloc[1])
    assert result.iloc[2] == 400.0
    assert np.isnan(result.iloc[3])


def test_clean_numeric_vectorized_ignores_already_numeric():
    """Ensure vectorisation avoids unnecessary processing on clean data types."""
    raw_series = pd.Series([10.5, 20.0, 30.1])
    result = clean_numeric_vectorized(raw_series)
    pd.testing.assert_series_equal(raw_series, result)


# =====================================================================
# Key Normalisation Tests
# =====================================================================


def test_clean_json_keys_removes_prefix():
    """Verify stripping of the redundant 'values.' prefix emitted by Node-RED."""
    df = pd.DataFrame(
        {"values.id": [1, 2], "values.weight": [500, 600], "standard_key": ["A", "B"]}
    )

    df_clean = clean_json_keys(df)

    expected_columns = ["id", "weight", "standard_key"]
    assert list(df_clean.columns) == expected_columns
