"""
Pure utility functions with no internal dependencies.
This module must not import anything from core/*.
"""

import pandas as pd


def clean_numeric_vectorized(series: pd.Series) -> pd.Series:
    """Clean and convert a Series to numeric using vectorised operations.

    Handles decimal commas (European locale) and coerces unparseable
    values to NaN instead of raising.

    Args:
        series: Input Series of any dtype.

    Returns:
        Float64 Series with invalid values set to NaN.
    """
    if pd.api.types.is_numeric_dtype(series):
        return series

    series_str = series.astype(str).str.replace(",", ".", regex=False)
    return pd.to_numeric(series_str, errors="coerce")


def clean_json_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Strip the ``values.`` prefix produced by flat JSON normalisation.

    Args:
        df: DataFrame whose column names may contain ``values.`` prefixes.

    Returns:
        DataFrame with cleaned column names (in-place rename).
    """
    df.columns = df.columns.str.replace("values.", "", regex=False)
    return df
