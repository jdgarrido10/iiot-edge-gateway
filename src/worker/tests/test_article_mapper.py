"""
Test suite for the Article Mapper: dynamic configuration loading, hot-reloads, and DataFrame enrichment.
"""

import pytest
import pandas as pd
import json
import os
import time
from pathlib import Path
from unittest.mock import patch

from alerts.article_mapper import ArticleMapper

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def config_file_path(tmp_path: Path) -> str:
    """Provide an isolated temporary path for the configuration file."""
    return str(tmp_path / "test_articles_map.json")


@pytest.fixture
def custom_config(config_file_path: str) -> str:
    """Pre-populate the temporary configuration file with test data."""
    data = {
        "description": "Test mapping",
        "default": "UNMAPPED_ITEM",
        "mappings": {"A-100": "Alpha Product", "B-200": "Beta Product"},
    }
    with open(config_file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return config_file_path


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Provide a standard payload for enrichment tests."""
    return pd.DataFrame(
        {
            "article_number": [
                "A-100",
                "B-200",
                " X-999 ",
                100,
            ]  # Includes spaces and numeric types
        }
    )


# =====================================================================
# Initialization & Setup Tests
# =====================================================================


def test_initialization_creates_default_config(config_file_path: str):
    """Ensure a default configuration is generated if the target file is missing."""
    assert not os.path.exists(config_file_path)

    # Instantiating the mapper should trigger the file creation
    ArticleMapper(config_path=config_file_path)

    assert os.path.exists(config_file_path)

    with open(config_file_path, "r", encoding="utf-8") as f:
        config = json.load(f)
        assert "mappings" in config
        assert "1001" in config["mappings"]


def test_initialization_loads_existing_config(custom_config: str):
    """Verify that existing configuration files are correctly parsed upon initialization."""
    mapper = ArticleMapper(config_path=custom_config)

    mapping = mapper.get_mapping()
    assert mapping["A-100"] == "Alpha Product"
    assert len(mapping) == 2


# =====================================================================
# Enrichment Logic Tests
# =====================================================================


def test_enrich_dataframe_applies_mappings(
    custom_config: str, sample_dataframe: pd.DataFrame
):
    """Verify that article codes are safely stringified, stripped, and mapped."""
    mapper = ArticleMapper(config_path=custom_config)

    enriched_df = mapper.enrich_dataframe(sample_dataframe, target_col="mapped_name")

    assert "mapped_name" in enriched_df.columns
    assert enriched_df["mapped_name"].iloc[0] == "Alpha Product"
    assert enriched_df["mapped_name"].iloc[1] == "Beta Product"


def test_enrich_dataframe_uses_default_for_unknowns(
    custom_config: str, sample_dataframe: pd.DataFrame
):
    """Ensure unmapped or unexpected codes cleanly fall back to the default value."""
    mapper = ArticleMapper(config_path=custom_config)

    enriched_df = mapper.enrich_dataframe(sample_dataframe, target_col="mapped_name")

    # " X-999 " (stripped) and 100 do not exist in the custom_config
    assert enriched_df["mapped_name"].iloc[2] == "UNMAPPED_ITEM"
    assert enriched_df["mapped_name"].iloc[3] == "UNMAPPED_ITEM"


def test_enrich_empty_dataframe_is_noop(custom_config: str):
    """Ensure empty datasets are returned immediately without processing to avoid pandas errors."""
    mapper = ArticleMapper(config_path=custom_config)
    empty_df = pd.DataFrame()

    result = mapper.enrich_dataframe(empty_df)

    assert result.empty


# =====================================================================
# Hot-Reload & Concurrency Tests
# =====================================================================


def test_hot_reload_on_file_change(custom_config: str):
    """Verify that modifying the config file updates the mapping at runtime."""
    mapper = ArticleMapper(config_path=custom_config)

    assert mapper.get_mapping().get("C-300") is None

    # Simulate an external file update
    new_data = {
        "description": "Updated mapping",
        "default": "NEW_DEFAULT",
        "mappings": {"A-100": "Alpha Product", "C-300": "Gamma Product"},
    }

    # Ensure mtime is strictly greater to trigger the reload
    time.sleep(0.02)
    with open(custom_config, "w", encoding="utf-8") as f:
        json.dump(new_data, f)

    updated_mapping = mapper.get_mapping()

    assert updated_mapping["C-300"] == "Gamma Product"
    assert mapper._default_value == "NEW_DEFAULT"


@patch("alerts.article_mapper.json.load")
def test_hot_reload_skips_unchanged_file(mock_json_load, custom_config: str):
    """Ensure the mapper does not perform unnecessary disk reads if mtime is unchanged."""

    # Assign a valid dictionary so the mock returns standard types to pandas
    mock_json_load.return_value = {
        "description": "Mocked mapping",
        "default": "UNMAPPED_ITEM",
        "mappings": {"A-100": "Alpha Product"},
    }

    mapper = ArticleMapper(config_path=custom_config)

    # Initial load happens during __init__, let's reset the mock counter
    mock_json_load.reset_mock()

    # Fetch mapping multiple times without modifying the file
    mapper.get_mapping()
    mapper.enrich_dataframe(pd.DataFrame({"article_number": ["A-100"]}))

    # The file wasn't touched, so json.load should not be called again
    mock_json_load.assert_not_called()
