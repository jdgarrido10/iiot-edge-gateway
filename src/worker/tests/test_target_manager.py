"""
Test suite for the Target Manager: default configuration creation, hot-reloading cache, and DataFrame enrichment.
"""

import json
import os
from unittest.mock import patch, mock_open
from pathlib import Path

import pandas as pd
import pytest

from core.target_manager import TargetManager

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def temp_config_path(tmp_path: Path) -> str:
    """Provide an isolated temporary path for the configuration file."""
    return str(tmp_path / "test_targets.json")


@pytest.fixture
def mock_targets_data() -> dict:
    """Provide a standard dictionary of mock targets for enrichment."""
    return {
        "100": 55.5,
        "Alpha Product": 120.0,
        "Bad Data": "Not a Number",
        "default": 15.0,
    }


# =====================================================================
# Initialization & Disk I/O Tests
# =====================================================================


def test_ensure_config_creates_missing(temp_config_path: str):
    """Ensure a default configuration is generated if the target file is missing."""
    assert not os.path.exists(temp_config_path)

    TargetManager(config_path=temp_config_path)

    assert os.path.exists(temp_config_path)
    with open(temp_config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert data == {"default": 20.0}


def test_ensure_config_skips_existing(temp_config_path: str):
    """Ensure existing configuration files are not overwritten during initialization."""
    os.makedirs(os.path.dirname(temp_config_path), exist_ok=True)
    with open(temp_config_path, "w", encoding="utf-8") as f:
        json.dump({"default": 99.9}, f)

    TargetManager(config_path=temp_config_path)

    with open(temp_config_path, "r", encoding="utf-8") as f:
        assert json.load(f)["default"] == 99.9


@patch("core.target_manager.os.makedirs")
def test_ensure_config_handles_creation_error(mock_makedirs, temp_config_path: str):
    """Ensure the manager does not crash if it lacks permissions to create the config file."""
    mock_makedirs.side_effect = OSError("Permission denied")

    manager = TargetManager(config_path=temp_config_path)

    assert manager.get_target_for_product("123") == 0.0


# =====================================================================
# Hot-Reload & Cache Logic Tests
# =====================================================================


@patch("core.target_manager.os.path.getmtime")
@patch("core.target_manager.open", new_callable=mock_open, read_data='{"100": 50.0}')
def test_load_targets_initial_cache(mock_file, mock_mtime):
    """Ensure targets are loaded into the cache on the first request."""
    mock_mtime.return_value = 100.0

    with patch("core.target_manager.os.path.exists", return_value=True):
        manager = TargetManager(config_path="/fake/path.json")
        targets = manager._load_targets()

    assert targets["100"] == 50.0
    mock_file.assert_called_once()


@patch("core.target_manager.os.path.getmtime")
@patch("core.target_manager.open", new_callable=mock_open)
def test_load_targets_hot_reload_on_mtime_change(mock_file, mock_mtime):
    """Ensure the cache reloads dynamically if the file's mtime increases."""
    mock_mtime.return_value = 100.0
    mock_file.return_value.read.return_value = '{"100": 50.0}'

    with patch("core.target_manager.os.path.exists", return_value=True):
        manager = TargetManager(config_path="/fake/path.json")
        assert manager._load_targets()["100"] == 50.0

        mock_mtime.return_value = 200.0
        mock_file.return_value.read.return_value = '{"100": 99.9}'

        assert manager._load_targets()["100"] == 99.9

        assert mock_file.call_count == 2


@patch("core.target_manager.os.path.getmtime")
def test_load_targets_handles_stat_error(mock_mtime):
    """Ensure the system gracefully returns the stale cache if the file becomes unreadable."""
    manager = TargetManager(config_path="/fake/path.json")
    manager._cache = {"100": 75.0}

    mock_mtime.side_effect = OSError("File missing")

    targets = manager._load_targets()
    assert targets["100"] == 75.0


@patch("core.target_manager.os.path.getmtime")
@patch("core.target_manager.open", new_callable=mock_open, read_data="{INVALID JSON}")
def test_load_targets_handles_json_error(mock_file, mock_mtime):
    """Ensure the system gracefully retains the old cache if the new file is corrupted."""
    mock_mtime.return_value = 100.0

    manager = TargetManager(config_path="/fake/path.json")
    manager._cache = {"100": 75.0}

    with patch("core.target_manager.os.path.exists", return_value=True):
        targets = manager._load_targets()

    assert targets["100"] == 75.0


# =====================================================================
# Single Lookup API Tests
# =====================================================================


@patch.object(TargetManager, "_load_targets")
def test_get_target_for_product(mock_load_targets, mock_targets_data):
    """Verify single-item target lookups and their fallback hierarchy."""
    mock_load_targets.return_value = mock_targets_data

    with patch("core.target_manager.os.path.exists", return_value=True):
        manager = TargetManager(config_path="/fake/path.json")

    assert manager.get_target_for_product("100") == 55.5

    assert manager.get_target_for_product("Unknown") == 15.0


@patch.object(TargetManager, "_load_targets")
def test_get_target_for_product_no_default(mock_load_targets):
    """Verify fallback to 0.0 if the 'default' key is completely missing from config."""
    mock_load_targets.return_value = {"100": 55.5}

    with patch("core.target_manager.os.path.exists", return_value=True):
        manager = TargetManager(config_path="/fake/path.json")

    assert manager.get_target_for_product("Unknown") == 0.0


# =====================================================================
# Vectorized DataFrame Enrichment Tests
# =====================================================================


@patch.object(TargetManager, "_load_targets")
def test_enrich_dataframe_priority_and_coercion(mock_load_targets, mock_targets_data):
    """Verify the vectorized mapping logic prioritizes numeric codes, handles strings, and coerces errors."""
    mock_load_targets.return_value = mock_targets_data

    with patch("core.target_manager.os.path.exists", return_value=True):
        manager = TargetManager(config_path="/fake/path.json")

    df = pd.DataFrame(
        {
            "article_number": [100, 999, 888, "Bad Data"],
            "article_name": ["Irrelevant", "Alpha Product", "Unknown", "Unknown"],
        }
    )

    enriched_df = manager.enrich_dataframe(df)

    assert enriched_df["target_value"].iloc[0] == 55.5

    assert enriched_df["target_value"].iloc[1] == 120.0

    assert enriched_df["target_value"].iloc[2] == 15.0

    assert enriched_df["target_value"].iloc[3] == 0.0


def test_enrich_empty_dataframe(temp_config_path: str):
    """Ensure empty datasets immediately return without throwing pandas KeyError exceptions."""
    manager = TargetManager(config_path=temp_config_path)
    empty_df = pd.DataFrame()

    result = manager.enrich_dataframe(empty_df)
    assert result.empty
