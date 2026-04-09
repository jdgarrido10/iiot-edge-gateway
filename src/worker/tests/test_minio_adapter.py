"""
Test suite for the Object Storage adapter: bucket initialization, archiving, and filename generation.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from adapters.minio_adapter import MinioAdapter
from config import Config

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def mock_config(monkeypatch: pytest.MonkeyPatch):
    """Isolate configuration to prevent real external connections."""
    monkeypatch.setattr(Config, "MINIO_ENDPOINT", "mock-blob-storage:9000")
    monkeypatch.setattr(Config, "MINIO_ROOT_USER", "mock-user")
    monkeypatch.setattr(Config, "MINIO_ROOT_PASSWORD", "mock-pass")
    monkeypatch.setattr(Config, "MINIO_SECURE", False)
    monkeypatch.setattr(Config, "BUCKET_NAME", "mock-archive-bucket")


@pytest.fixture
def mock_blob_client():
    """Mock the external blob storage client to intercept bucket and object operations."""
    with patch("adapters.minio_adapter.Minio") as mock_client_class:
        mock_instance = MagicMock()
        mock_client_class.return_value = mock_instance

        # Default behavior: simulate bucket does not exist to test creation
        mock_instance.bucket_exists.return_value = False

        yield mock_client_class, mock_instance


@pytest.fixture
def adapter(mock_config, mock_blob_client) -> MinioAdapter:
    """Instantiate the adapter with mocked external dependencies."""
    return MinioAdapter()


@pytest.fixture
def standard_payload() -> pd.DataFrame:
    """Provide a standard dataset with metadata columns for routing and tagging."""
    return pd.DataFrame(
        {
            "identity": [1, 2],
            "batch_code": ["BATCH_001", "BATCH_001"],
            "article_name": ["Product Alpha", "Product Alpha"],
            "quality_status": ["OK", "REJECTED"],
            "reject_flag": [0, 1],
            "mqtt_topic": ["factory/line_1", "factory/line_1"],
        }
    )


# =====================================================================
# Initialization & Setup Tests
# =====================================================================


def test_initialization_creates_bucket_if_missing(mock_config, mock_blob_client):
    """Ensure the adapter creates the target bucket if it does not already exist."""
    _, mock_instance = mock_blob_client

    __adapter = MinioAdapter()

    mock_instance.bucket_exists.assert_called_once_with("mock-archive-bucket")
    mock_instance.make_bucket.assert_called_once_with("mock-archive-bucket")
    mock_instance.set_bucket_lifecycle.assert_called_once()
    mock_instance.set_bucket_versioning.assert_called_once()


def test_initialization_skips_bucket_creation_if_exists(mock_config, mock_blob_client):
    """Ensure the adapter respects existing buckets and skips creation."""
    _, mock_instance = mock_blob_client
    mock_instance.bucket_exists.return_value = True

    _adapter = MinioAdapter()

    mock_instance.make_bucket.assert_not_called()


def test_initialization_survives_policy_errors(mock_config, mock_blob_client):
    """Verify that failures in advanced policies (lifecycle/versioning) do not crash the app."""
    _, mock_instance = mock_blob_client
    mock_instance.set_bucket_lifecycle.side_effect = Exception("Policy Denied")

    # Should instantiate without raising
    _adapter = MinioAdapter()

    # Verify it attempted to call the next configuration step regardless of the previous error
    mock_instance.set_bucket_versioning.assert_called_once()


# =====================================================================
# Public Write API Tests
# =====================================================================


def test_save_dataframe_as_csv_success(
    adapter: MinioAdapter, mock_blob_client, standard_payload: pd.DataFrame
):
    """Verify dataframes are correctly encoded to CSV and shipped with metadata tags."""
    _, mock_instance = mock_blob_client

    bytes_written = adapter.save_dataframe_as_csv(
        standard_payload, "packaging/test.csv"
    )

    assert bytes_written > 0
    mock_instance.put_object.assert_called_once()

    call_kwargs = mock_instance.put_object.call_args.kwargs
    assert call_kwargs["bucket_name"] == "mock-archive-bucket"
    assert call_kwargs["object_name"] == "packaging/test.csv"
    assert call_kwargs["content_type"] == "application/csv"
    assert "Tags" in str(type(call_kwargs["tags"])), (
        "Failed to attach Object Storage tags."
    )


def test_save_dataframe_as_csv_empty_skip(adapter: MinioAdapter, mock_blob_client):
    """Ensure empty datasets are skipped to save network bandwidth and storage."""
    _, mock_instance = mock_blob_client
    empty_df = pd.DataFrame()

    bytes_written = adapter.save_dataframe_as_csv(empty_df, "packaging/empty.csv")

    assert bytes_written == 0
    mock_instance.put_object.assert_not_called()


def test_save_json_success(adapter: MinioAdapter, mock_blob_client):
    """Verify dictionary payloads are strictly serialized and stored."""
    _, mock_instance = mock_blob_client
    payload = {"status": "active", "workers": 5}

    success = adapter.save_json("config/state.json", payload)

    assert success is True
    mock_instance.put_object.assert_called_once()

    call_kwargs = mock_instance.put_object.call_args.kwargs
    assert call_kwargs["content_type"] == "application/json"


def test_save_worker_log_formatting(adapter: MinioAdapter, mock_blob_client):
    """Verify worker logs generate the correct directory structure and schema."""
    _, mock_instance = mock_blob_client

    adapter.save_worker_log("Line_Alpha", 4)

    mock_instance.put_object.assert_called_once()
    object_name = mock_instance.put_object.call_args.kwargs["object_name"]

    assert "packaging/Manual_Config/Line_Alpha/workers/" in object_name
    assert "workers_change_4.csv" in object_name


# =====================================================================
# Filename & Tagging Helpers
# =====================================================================


def test_get_smart_filename_standard(
    adapter: MinioAdapter, standard_payload: pd.DataFrame
):
    """Verify filenames dynamically adapt to batch, product, and quality status."""
    filename = adapter.get_smart_filename(standard_payload)

    assert "BATCH_001" in filename
    assert "ProductAlpha" in filename
    assert "WARNING" in filename  # Because the payload has a 'REJECTED' row
    assert filename.endswith(".csv")


def test_get_smart_filename_missing_columns(adapter: MinioAdapter):
    """Ensure missing columns gracefully fall back to default indicators."""
    df_bare = pd.DataFrame({"identity": [1, 2], "net_weight": [1.5, 2.0]})

    filename = adapter.get_smart_filename(df_bare)

    assert "NoBatch" in filename
    assert "UnknownProd" in filename
    assert "RAW" in filename


def test_build_tags_extraction(adapter: MinioAdapter, standard_payload: pd.DataFrame):
    """Verify metadata extraction securely maps DataFrame values to Blob tags."""
    tags = adapter._build_tags(standard_payload)

    assert tags["BatchId"] == "BATCH_001"
    assert tags["QualityStatus"] == "WARNING"
    assert tags["HardwareReject"] == "TRUE"
    assert tags["Line"] == "factory_line_1"
