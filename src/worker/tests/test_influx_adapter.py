"""
Test suite for the Time Series Database adapter: write formatting, error handling, and circuit breaker protection.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from adapters.influx_adapter import InfluxAdapter
from core.circuit_breaker import CircuitState
from config import Config

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def mock_config(monkeypatch: pytest.MonkeyPatch):
    """Isolate configuration to prevent real external connections."""
    monkeypatch.setattr(Config, "INFLUX_URL", "http://mock-tsdb:8086")
    monkeypatch.setattr(Config, "INFLUX_TOKEN", "mock-token")
    monkeypatch.setattr(Config, "INFLUX_ORG", "mock-org")
    monkeypatch.setattr(Config, "INFLUX_BUCKET", "mock-bucket")
    monkeypatch.setattr(Config, "INFLUX_TIMEOUT", 1000)
    # Lower the threshold to speed up circuit breaker tests
    monkeypatch.setattr(Config, "CIRCUIT_BREAKER_THRESHOLD", 2)
    monkeypatch.setattr(Config, "CIRCUIT_BREAKER_TIMEOUT", 0.5)


@pytest.fixture
def mock_tsdb_client():
    """Mock the external database client to intercept write calls."""
    with patch("adapters.influx_adapter.InfluxDBClient") as mock_client_class:
        mock_instance = MagicMock()
        mock_write_api = MagicMock()

        # Chain the mocks
        mock_instance.write_api.return_value = mock_write_api
        mock_client_class.return_value = mock_instance

        yield mock_client_class, mock_instance, mock_write_api


@pytest.fixture
def adapter(mock_config, mock_tsdb_client) -> InfluxAdapter:
    """Instantiate the adapter with mocked external dependencies."""
    return InfluxAdapter()


@pytest.fixture
def sample_payload() -> pd.DataFrame:
    """Provide a standardized valid dataset simulating industrial output."""
    return pd.DataFrame(
        {
            "identity": [100, 101],
            "timestamp": ["2024-01-01T10:00:00Z", "2024-01-01T10:01:00Z"],
            "net_weight": [1.5, 2.0],
            "active_workers": [3, 4],
            "reject_flag": [0, 1],
            "quality_status": ["ok", "rejected"],
            "linea": ["L1", "L1"],
        }
    )


# =====================================================================
# Validation & Formatting Tests
# =====================================================================


def test_write_empty_or_none_dataframe(adapter: InfluxAdapter, mock_tsdb_client):
    """Ensure empty or None datasets are safely ignored without network calls."""
    _, _, mock_write_api = mock_tsdb_client

    assert adapter.write_dataframe(None) == 0
    assert adapter.write_dataframe(pd.DataFrame()) == 0
    mock_write_api.write.assert_not_called()


def test_write_missing_identity_column(adapter: InfluxAdapter, mock_tsdb_client):
    """Ensure records missing the primary synchronization key are rejected."""
    _, _, mock_write_api = mock_tsdb_client
    df = pd.DataFrame({"timestamp": ["2024-01-01T10:00:00Z"], "net_weight": [1.5]})

    assert adapter.write_dataframe(df) == 0
    mock_write_api.write.assert_not_called()


def test_write_successful_formatting_and_transmission(
    adapter: InfluxAdapter, mock_tsdb_client, sample_payload: pd.DataFrame
):
    """Verify standard write behavior: type coercion, tag extraction, and transmission."""
    _, _, mock_write_api = mock_tsdb_client

    records_written = adapter.write_dataframe(sample_payload)

    assert records_written == 2
    mock_write_api.write.assert_called_once()

    # Extract the kwargs passed to the database write method
    call_kwargs = mock_write_api.write.call_args.kwargs
    written_df = call_kwargs["record"]

    # Verify internal dataframe transformations
    assert "timestamp" not in written_df.columns, (
        "Timestamp should be converted to a DatetimeIndex."
    )
    assert written_df.index.name == "timestamp", (
        "DatetimeIndex should retain the 'timestamp' name."
    )
    assert written_df["reject_flag"].dtype == int, (
        "Integer fields must be strictly cast to int."
    )
    assert written_df["quality_status"].iloc[0] == "OK", (
        "String tags must be normalized (uppercase/stripped)."
    )


def test_write_generates_synthetic_index_if_missing_timestamp(
    adapter: InfluxAdapter, mock_tsdb_client, sample_payload: pd.DataFrame
):
    """Ensure a fallback index is created if the timestamp column is missing."""
    _, _, mock_write_api = mock_tsdb_client

    # Remove timestamp to trigger the fallback logic
    df_no_ts = sample_payload.drop(columns=["timestamp"])

    records_written = adapter.write_dataframe(df_no_ts)
    assert records_written == 2

    call_kwargs = mock_write_api.write.call_args.kwargs
    written_df = call_kwargs["record"]

    assert isinstance(written_df.index, pd.DatetimeIndex), (
        "Failed to generate synthetic DatetimeIndex."
    )


# =====================================================================
# Circuit Breaker & Resilience Tests
# =====================================================================


def test_circuit_breaker_opens_on_db_failure(
    adapter: InfluxAdapter, mock_tsdb_client, sample_payload: pd.DataFrame
):
    """Verify that repeated external failures open the circuit to prevent thread pile-ups."""
    _, _, mock_write_api = mock_tsdb_client

    # Simulate an external network exception
    mock_write_api.write.side_effect = Exception("Simulated External Timeout")

    # Call 1: Fails, records error
    assert adapter.write_dataframe(sample_payload) == 0
    # Call 2: Fails, records error (Threshold reached = 2)
    assert adapter.write_dataframe(sample_payload) == 0

    assert adapter.circuit_breaker.state == CircuitState.OPEN

    # Subsequent calls should fast-fail without triggering the network
    mock_write_api.write.reset_mock()
    assert adapter.write_dataframe(sample_payload) == 0

    # Verify no further network attempts were made while the circuit is open
    mock_write_api.write.assert_not_called()


def test_graceful_shutdown(adapter: InfluxAdapter, mock_tsdb_client):
    """Ensure active client connections and write buffers are properly flushed and closed."""
    _, mock_instance, mock_write_api = mock_tsdb_client

    adapter.close()

    mock_write_api.close.assert_called_once()
    mock_instance.close.assert_called_once()
