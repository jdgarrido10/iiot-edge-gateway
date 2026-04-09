"""
Test suite for the Worker Manager: atomic configuration updates, asynchronous Object Storage logging, and DataFrame enrichment.
"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from core.worker_manager import WorkerManager

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def mock_object_storage():
    """Mock for the Object Storage adapter used for async logging."""
    return MagicMock()


@pytest.fixture
def temp_config_path(tmp_path: Path) -> str:
    """Provide an isolated temporary path for the configuration file."""
    return str(tmp_path / "test_workers.json")


@pytest.fixture
def manager(mock_object_storage, temp_config_path):
    """Provide a WorkerManager instance with isolated storage and mocked max limits."""
    with patch("core.worker_manager.Config") as mock_config:
        mock_config.MAX_WORKERS_PER_LINE = 10
        # Initialize the manager
        wm = WorkerManager(
            minio_adapter=mock_object_storage, config_path=temp_config_path
        )
        yield wm


# =====================================================================
# Initialization & Internal Loader Tests
# =====================================================================


def test_validate_etl_config_initializes_defaults(manager):
    """Ensure the manager injects safe default config parameters if they are missing."""
    with patch("core.worker_manager.etl_config") as mock_etl:
        del mock_etl.DEFAULT_WORKERS_PER_LINE
        manager._validate_etl_config()
        assert hasattr(mock_etl, "DEFAULT_WORKERS_PER_LINE")
        assert mock_etl.DEFAULT_WORKERS_PER_LINE["default"] == 1


def test_load_config_missing_file(manager, temp_config_path):
    """Ensure loading a missing configuration returns an empty dictionary."""
    assert not os.path.exists(temp_config_path)
    assert manager._load_config() == {}


@patch("core.worker_manager.open")
def test_load_config_handles_read_error(mock_file, manager):
    """Ensure corrupted files or permission errors gracefully return an empty dictionary."""
    with patch("core.worker_manager.os.path.exists", return_value=True):
        mock_file.side_effect = Exception("Simulated read error")
        assert manager._load_config() == {}


# =====================================================================
# Update & Persistence Tests
# =====================================================================


def test_update_worker_count_invalid_range(manager):
    """Ensure the system rejects counts below 0 or above MAX_WORKERS_PER_LINE."""
    # Test below 0
    assert manager.update_worker_count("Line_1", -1) is False

    # Test above mocked MAX_WORKERS_PER_LINE (10)
    assert manager.update_worker_count("Line_1", 15) is False


def test_update_worker_count_success_atomic_write(manager, temp_config_path):
    """Verify that valid updates are written atomically and queued for async logging."""
    assert manager.update_worker_count("Line_1", 4) is True

    # Verify file was written correctly
    assert os.path.exists(temp_config_path)
    with open(temp_config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert data["Line_1"] == 4

    # Verify the async executor received the logging task
    # We allow the threadpool a tiny fraction of a second to dispatch
    manager._io_executor.shutdown(wait=True)
    manager.minio_adapter.save_worker_log.assert_called_once_with("Line_1", 4)


@patch("core.worker_manager.open")
def test_update_worker_count_handles_read_before_write_error(
    mock_file, manager, temp_config_path
):
    """Ensure that if the existing config is unreadable, it is safely overwritten rather than crashing."""
    with patch("core.worker_manager.os.path.exists", return_value=True):
        mock_file.side_effect = Exception("Corrupt existing config")

        # The update should still succeed by writing a fresh config
        assert manager.update_worker_count("Line_1", 3) is True


@patch("core.worker_manager.os.replace")
def test_update_worker_count_handles_write_error(
    mock_replace, manager, temp_config_path
):
    """Ensure filesystem write failures (e.g., disk full) return False."""
    mock_replace.side_effect = OSError("Disk full")

    assert manager.update_worker_count("Line_1", 5) is False


def test_get_all_workers_wraps_loader(manager, temp_config_path):
    """Ensure get_all_workers correctly retrieves the dictionary."""
    manager.update_worker_count("Line_A", 2)
    manager.update_worker_count("Line_B", 3)

    workers = manager.get_all_workers()
    assert workers["Line_A"] == 2
    assert workers["Line_B"] == 3


# =====================================================================
# DataFrame Enrichment Tests
# =====================================================================


@patch.object(WorkerManager, "_load_config")
@patch("core.worker_manager.etl_config")
def test_enrich_dataframe_standard_behavior(mock_etl, mock_load, manager):
    """Verify that mapped lines receive their config values and unknown lines get defaults."""
    mock_load.return_value = {"Line_1": 4}
    mock_etl.DEFAULT_WORKERS_PER_LINE = {"Line_2": 8, "default": 2}

    df = pd.DataFrame({"linea": ["Line_1", "Line_2", "Unknown_Line", "  Line_1  "]})

    enriched_df = manager.enrich_dataframe(df)

    # Line_1 is in the config file
    assert enriched_df["active_workers"].iloc[0] == 4
    # Line_2 is missing from config, falls back to etl_config line-specific default
    assert enriched_df["active_workers"].iloc[1] == 8
    # Unknown_Line is missing entirely, falls back to etl_config global default
    assert enriched_df["active_workers"].iloc[2] == 2
    # Ensure stripping works on padded strings
    assert enriched_df["active_workers"].iloc[3] == 4


@patch.object(WorkerManager, "_load_config")
def test_enrich_dataframe_clamps_to_max(mock_load, manager):
    """Ensure that if the config file was manually edited to exceed maximum limits, it gets clamped."""
    mock_load.return_value = {"Line_1": 99}  # Exceeds mocked MAX of 10

    df = pd.DataFrame({"linea": ["Line_1"]})
    enriched_df = manager.enrich_dataframe(df)

    assert enriched_df["active_workers"].iloc[0] == 10


def test_enrich_dataframe_missing_linea_column(manager):
    """Ensure DataFrames missing the linea column gracefully assign the default to all rows."""
    with patch("core.worker_manager.etl_config") as mock_etl:
        mock_etl.DEFAULT_WORKERS_PER_LINE = {"default": 5}

        df = pd.DataFrame({"other_column": [1, 2, 3]})
        enriched_df = manager.enrich_dataframe(df)

        assert all(enriched_df["active_workers"] == 5)


def test_enrich_empty_dataframe(manager):
    """Ensure empty datasets immediately return without throwing pandas exceptions."""
    df = pd.DataFrame()
    result = manager.enrich_dataframe(df)
    assert result.empty


# =====================================================================
# Lifecycle & Shutdown Tests
# =====================================================================


def test_shutdown_success(manager):
    """Verify that the async executor shuts down cleanly."""
    # Since we can't easily assert on standard library internals without deep patching,
    # we verify it doesn't raise an exception during standard usage.
    manager.shutdown()


@patch("core.worker_manager.ThreadPoolExecutor.shutdown")
def test_shutdown_handles_exception(mock_shutdown, manager):
    """Verify that an error during thread pool destruction is caught and logged."""
    mock_shutdown.side_effect = Exception("Thread lock error")

    # Should not raise
    manager.shutdown()
