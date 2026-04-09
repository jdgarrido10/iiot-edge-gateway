"""
Test suite for the Data Pipeline orchestrator: flow control, enrichment integration, and storage dispatch.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from core.pipeline import DataPipeline

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def mock_minio():
    """Mock for Object Storage adapter."""
    adapter = MagicMock()
    adapter.get_smart_filename.return_value = "smart_file.csv"
    adapter.save_dataframe_as_csv.return_value = 1024  # Returns bytes written
    return adapter


@pytest.fixture
def mock_influx():
    """Mock for Time-Series Database adapter."""
    adapter = MagicMock()
    adapter.write_dataframe.return_value = 5  # Returns count of metrics written
    return adapter


@pytest.fixture
def mock_state():
    """Mock for cross-restart deduplication manager."""
    manager = MagicMock()
    manager.is_already_processed.return_value = (
        False  # By default, nothing is duplicate
    )
    return manager


@pytest.fixture
def mock_worker():
    """Mock for line worker enrichment."""
    manager = MagicMock()
    manager.enrich_dataframe = MagicMock(side_effect=lambda df: df.copy())
    return manager


@pytest.fixture
def mock_target():
    """Mock for production target enrichment."""
    manager = MagicMock()
    manager.enrich_dataframe = MagicMock(side_effect=lambda df: df.copy())
    return manager


@pytest.fixture
def pipeline_instance(mock_minio, mock_influx, mock_state, mock_worker, mock_target):
    """
    Creates an isolated DataPipeline. Mocks internal file-based mappers
    and monitoring classes to prevent disk/network I/O during __init__.
    """
    with (
        patch("core.pipeline.ArticleMapper"),
        patch("core.pipeline.AlertNotifier"),
        patch("core.pipeline.ProductionMonitor") as mock_monitor,
    ):
        # Configure the mock monitor to safely pass through DataFrames
        mock_monitor_instance = mock_monitor.return_value
        mock_monitor_instance.process_chunk.side_effect = lambda df, topic: (
            df.copy(),
            [],
        )
        # Provide a dummy mapping for metadata propagation
        mock_monitor_instance.mapper.get_mapping.return_value = {
            "ART-1": "Refactored-ART-1"
        }

        pipeline = DataPipeline(
            minio_svc=mock_minio,
            influx_svc=mock_influx,
            state_manager=mock_state,
            worker_manager=mock_worker,
            target_manager=mock_target,
        )
        yield pipeline


@pytest.fixture
def sample_payload():
    """Standardized wrapped buffer input imitating the BufferManager output."""
    return [
        {"payload": {"identity": 1, "value": 100}},
        {"payload": {"identity": 2, "value": 200}},
    ]


# =====================================================================
# Main Pipeline Flow Tests
# =====================================================================


@patch("core.pipeline.process_node_red_payload")
def test_run_pipeline_happy_path(mock_etl, pipeline_instance, sample_payload):
    """Verify that a successful batch traverses all stages and saves to both targets."""

    # Mock the ETL stage to return two valid DataFrames
    df_tsdb = pd.DataFrame(
        {"identity": [1, 2], "timestamp": [datetime.now(timezone.utc)] * 2}
    )
    df_obj = pd.DataFrame(
        {"identity": [1, 2], "timestamp": [datetime.now(timezone.utc)] * 2}
    )
    mock_etl.return_value = (df_tsdb, df_obj)

    result = pipeline_instance.run_pipeline("test/topic", sample_payload)

    assert result is True
    pipeline_instance.influx.write_dataframe.assert_called_once()
    pipeline_instance.minio.save_dataframe_as_csv.assert_called_once()
    pipeline_instance.state_manager.save_checkpoint.assert_called_once()


@patch("core.pipeline.process_node_red_payload")
def test_run_pipeline_empty_etl_aborts_cleanly(
    mock_etl, pipeline_instance, sample_payload
):
    """Ensure the pipeline exits safely and returns True if ETL yields no valid data."""
    mock_etl.return_value = (None, None)

    result = pipeline_instance.run_pipeline("test/topic", sample_payload)

    assert result is True
    pipeline_instance.influx.write_dataframe.assert_not_called()


@patch("core.pipeline.process_node_red_payload")
def test_run_pipeline_all_duplicates_aborts_cleanly(
    mock_etl, pipeline_instance, sample_payload
):
    """Ensure the pipeline stops processing if the StateManager flags all rows as duplicates."""
    df_tsdb = pd.DataFrame({"identity": [1, 2]})
    mock_etl.return_value = (df_tsdb, None)

    pipeline_instance.state_manager.is_already_processed.return_value = True

    with patch.object(
        pipeline_instance.worker_manager, "enrich_dataframe"
    ) as mock_enrich:
        result = pipeline_instance.run_pipeline("test/topic", sample_payload)

        assert result is True
        mock_enrich.assert_not_called()

    pipeline_instance.influx.write_dataframe.assert_not_called()


@patch("core.pipeline.process_node_red_payload")
def test_run_pipeline_handles_enrichment_exceptions(
    mock_etl, pipeline_instance, sample_payload
):
    """Ensure that non-critical enrichment errors do not halt the entire pipeline."""
    df_tsdb = pd.DataFrame({"identity": [1, 2]})
    mock_etl.return_value = (df_tsdb, None)

    pipeline_instance.worker_manager.enrich_dataframe.side_effect = Exception(
        "Worker DB down"
    )
    pipeline_instance.prod_monitor.process_chunk.side_effect = Exception(
        "Monitor crash"
    )

    result = pipeline_instance.run_pipeline("test/topic", sample_payload)

    assert result is True
    pipeline_instance.influx.write_dataframe.assert_called_once()


@patch("core.pipeline.process_node_red_payload")
def test_run_pipeline_storage_failures_return_false(
    mock_etl, pipeline_instance, sample_payload
):
    """Ensure catastrophic storage failures return False to trigger higher-level DLQ/Retries."""
    df_tsdb = pd.DataFrame({"identity": [1]})
    df_obj = pd.DataFrame({"identity": [1]})
    mock_etl.return_value = (df_tsdb, df_obj)

    pipeline_instance.minio.save_dataframe_as_csv.return_value = 0

    result = pipeline_instance.run_pipeline("test/topic", sample_payload)

    assert result is False
    pipeline_instance.influx.write_dataframe.assert_not_called()
    pipeline_instance.state_manager.save_checkpoint.assert_not_called()


# =====================================================================
# Private Helper Logic Tests
# =====================================================================


def test_normalize_timestamps_timezone_coercion(pipeline_instance):
    """Ensure raw datetimes are cleanly localized or converted to UTC."""
    # Create DF with naive timestamps
    df = pd.DataFrame({"timestamp": [datetime(2026, 4, 8, 10, 0)]})

    norm_df = pipeline_instance._normalize_timestamps(df)

    assert norm_df["timestamp"].dt.tz is not None
    assert str(norm_df["timestamp"].dt.tz) == "UTC"


def test_propagate_batch_metadata(pipeline_instance):
    """Ensure monitoring tags (batch IDs, cleaned articles) sync cleanly to the Object Storage DF."""
    df_tsdb = pd.DataFrame(
        {
            "record_uuid": ["uuid-123"],
            "batch_instance_id": ["BATCH_99"],
            "batch_sequence": [1],
            "number_refactor": ["Refactored-ART-1"],
        }
    )

    df_obj = pd.DataFrame({"record_uuid": ["uuid-123"], "article_number": ["ART-1"]})

    merged = pipeline_instance._propagate_batch_metadata(df_tsdb, df_obj)

    assert merged["batch_instance_id"].iloc[0] == "BATCH_99"
    assert merged["number_refactor"].iloc[0] == "Refactored-ART-1"


def test_save_to_minio_path_construction(pipeline_instance):
    """Ensure the Object Storage adapter receives correctly formatted hierarchical paths."""
    # Data containing all hierarchical hints
    df = pd.DataFrame(
        {
            "mqtt_topic": ["raw/data"],
            "linea": ["Line_1"],
            "timestamp": [datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)],
            "number_refactor": ["Alpha_Prod"],
            "batch_instance_id": ["B-100"],
        }
    )

    pipeline_instance._save_to_minio("fallback/topic", df)

    # Verify the constructed path (should use Madrid time for the folder structure)
    # 12:00 UTC -> 14:00 CEST (April) -> Same day
    expected_path = (
        "packaging/raw_data/Line_1/2026/04/08/Alpha_Prod/B-100/smart_file.csv"
    )

    pipeline_instance.minio.save_dataframe_as_csv.assert_called_once_with(
        df, expected_path
    )


def test_save_checkpoint_multiple_topics(pipeline_instance):
    """Ensure distinct sub-topics within a single batch are checkpointed independently."""
    df = pd.DataFrame(
        {"mqtt_topic": ["topic/A", "topic/B", "topic/A"], "identity": [1, 2, 3]}
    )

    pipeline_instance._save_checkpoint(df, "default/topic")

    assert pipeline_instance.state_manager.save_checkpoint.call_count == 2


# =====================================================================
# Diagnostic Tests
# =====================================================================


def test_get_production_status(pipeline_instance):
    """Verify that external API endpoints can pull state directly from the Monitor."""
    pipeline_instance.prod_monitor.state_memory = {"test/topic": {"status": "running"}}

    status = pipeline_instance.get_production_status("test/topic")

    assert status["status"] == "active"
    assert status["production_monitor"]["status"] == "running"
    assert "timestamp_local" in status
