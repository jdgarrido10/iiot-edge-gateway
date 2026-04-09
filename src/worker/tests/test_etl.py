"""
Test suite for ETL operations: schema validation, jitter logic, and fault tolerance.
"""

import pytest
import pandas as pd

from core.etl import process_node_red_payload

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def raw_industrial_payload() -> list[dict]:
    """Simulate a raw, potentially dirty JSON payload from factory sensors.

    Includes edge cases: exact timestamp collisions and non-numeric garbage data.
    """
    return [
        {
            "id": "abc_001",
            "identity": 300001,
            "timestamp": "10:30",  # Industrial scale format (HH:MM)
            "net_weight": 1.0045,
            "article_name": "Product_Alpha",
            "article_number": "A-1001",
            "device_name": "Line_A",
            "quality_status": "OK",
            "reject_flag": 0,
            "batch_code": "BATCH-Line_A-001",
            "mqtt_topic": "factory/zone_1/production",
        },
        {
            "id": "abc_002",
            "identity": 300002,
            "timestamp": "10:30",  # COLLISION: Exact same time
            "net_weight": 0.5012,
            "article_name": "Product_Beta",
            "article_number": "A-1002",
            "device_name": "Line_B",
            "quality_status": "OK",
            "reject_flag": 0,
            "batch_code": "BATCH-Line_B-001",
            "mqtt_topic": "factory/zone_1/production",
        },
        {
            "id": "abc_003",
            "identity": 300003,
            "timestamp": "25:99",  # Invalid time format
            "net_weight": "sensor_error",  # Non-numeric garbage
            "article_name": "Product_Gamma",
            "article_number": "A-1003",
            "device_name": "Line_A",
            "quality_status": "REJECTED",
            "reject_flag": 1,
            "batch_code": "BATCH-Line_A-001",
            "mqtt_topic": "factory/zone_1/production",
        },
    ]


# =====================================================================
# Transformation & Jitter Tests
# =====================================================================


def test_etl_schema_and_type_coercion(raw_industrial_payload: list[dict]):
    """Validate that the ETL produces a safe schema and coerces types correctly."""
    df_influx, df_minio = process_node_red_payload(raw_industrial_payload)

    assert isinstance(df_influx, pd.DataFrame)
    assert isinstance(df_minio, pd.DataFrame)

    # The garbage record (index 2) must be safely dropped
    assert len(df_influx) == 2, "ETL failed to discard malformed data."

    assert pd.api.types.is_numeric_dtype(df_influx["net_weight"]), (
        "ETL failed to enforce numeric types on the weight column."
    )

    assert df_influx["net_weight"].iloc[0] == 1.0045


def test_etl_timestamp_parsing_and_jitter(raw_industrial_payload: list[dict]):
    """Verify jitter injection to prevent TSDB (InfluxDB) data loss.

    InfluxDB silently overwrites points with identical tags and timestamps.
    The ETL must parse the HH:MM string and inject microsecond jitter based on identity.
    """
    df_influx, _ = process_node_red_payload(raw_industrial_payload)

    ts1 = df_influx.iloc[0]["timestamp"]
    ts2 = df_influx.iloc[1]["timestamp"]

    assert isinstance(ts1, pd.Timestamp)

    # Times must be separated at the microsecond level
    assert ts1 != ts2, "Critical Failure: Time collision not resolved by jitter."

    # But the deviation must be less than 1 second to preserve analytics accuracy
    diferencia = abs((ts2 - ts1).total_seconds())
    assert diferencia < 1.0, "Jitter distorted the actual timestamp excessively."


def test_etl_resilience_to_garbage_data():
    """Chaos testing: Ensure the ETL never crashes on nulls or empty arrays."""
    df_in, df_out = process_node_red_payload([])
    assert df_in.empty

    garbage_payload = [{"invalid_key": 123}, {"identity": None, "net_weight": ""}]

    try:
        df_in, df_out = process_node_red_payload(garbage_payload)
        assert isinstance(df_in, pd.DataFrame)
    except Exception as e:
        pytest.fail(
            f"Vulnerability: ETL crashed when processing corrupt data. Error: {e}"
        )
