from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, mock_open

import pytest
import pandas as pd
from alerts.production_monitor import ProductionMonitor

# === FIXTURES ===


@pytest.fixture
def mock_minio():
    return MagicMock()


@pytest.fixture
def mock_mapper():
    mapper = MagicMock()
    mapper.enrich_dataframe = lambda df, **kwargs: df  # Pass-through for simplicity
    return mapper


@pytest.fixture
def mock_notifier():
    return MagicMock()


@pytest.fixture
def isolated_monitor(mock_minio, mock_mapper, mock_notifier):
    """
    Creates a ProductionMonitor with intercepted background threads
    and mocked disk operations to ensure total isolation.
    """
    with patch("alerts.production_monitor.threading.Thread.start"):
        with patch.object(ProductionMonitor, "_restore_state_from_disk"):
            monitor = ProductionMonitor(
                minio_adapter=mock_minio,
                mapper=mock_mapper,
                alert_notifier=mock_notifier,
            )
            yield monitor


# === WATCHDOG & SHIFT TESTS ===


@patch("alerts.production_monitor.datetime")
def test_watchdog_suppressed_outside_working_hours(mock_datetime, isolated_monitor):
    """Ensure no stop alerts are triggered outside of defined shift hours."""
    # Mock time to 21:00 Madrid (Outside SHIFT_END)
    mock_now = datetime(2026, 4, 8, 21, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = mock_now

    # Inject a dormant state
    isolated_monitor.state_memory["TEST/LINE_1"] = {
        "type": "LOCAL_STATE",
        "stop_alert_sent": False,
        "current": {
            "end_time": mock_now - timedelta(hours=2),
            "number_refactor": "ART-001",
        },
    }

    isolated_monitor._check_inactivity()

    # Assert alert was NOT dispatched
    isolated_monitor.alert_notifier.send_alert.assert_not_called()


@patch("alerts.production_monitor.datetime")
def test_watchdog_triggers_stop_alert_during_shift(mock_datetime, isolated_monitor):
    """Ensure stop alerts fire correctly when a line is silent during active shifts."""
    # Mock time to 10:00 Madrid (During shift, not lunch)
    mock_now = datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = mock_now

    # Line has been silent for 15 minutes (Threshold is 10 min)
    isolated_monitor.state_memory["TEST/LINE_1"] = {
        "type": "LOCAL_STATE",
        "stop_alert_sent": False,
        "last_connection_lag_seconds": 0.0,
        "current": {
            "end_time": mock_now - timedelta(minutes=15),
            "number_refactor": "ART-002",
            "start_time": mock_now - timedelta(minutes=60),
        },
    }

    # Catch the snapshot save call to avoid actual disk I/O
    with patch.object(isolated_monitor, "_save_state_snapshot_dual"):
        isolated_monitor._check_inactivity()

    # Assert alert WAS dispatched
    isolated_monitor.alert_notifier.send_alert.assert_called_once()
    assert isolated_monitor.state_memory["TEST/LINE_1"]["stop_alert_sent"] is True


@patch("alerts.production_monitor.datetime")
def test_watchdog_suppressed_due_to_wifi_lag(mock_datetime, isolated_monitor):
    """Ensure post-WiFi reconnection bursts do not trigger false stop alerts."""
    mock_now = datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = mock_now

    # Line is silent for 30 minutes, BUT lag is high
    isolated_monitor.state_memory["TEST/LINE_1"] = {
        "type": "LOCAL_STATE",
        "stop_alert_sent": False,
        "last_connection_lag_seconds": 2000.0,  # Exceeds LAG_THRESHOLD_SECONDS (900)
        "current": {
            "end_time": mock_now - timedelta(minutes=30),
            "number_refactor": "ART-WIFI",
        },
    }

    isolated_monitor._check_inactivity()

    # Assert alert was suppressed
    isolated_monitor.alert_notifier.send_alert.assert_not_called()


# === PERSISTENCE & DISK I/O TESTS ===


@patch("alerts.production_monitor.Path.mkdir")
@patch("alerts.production_monitor.open", new_callable=mock_open)
def test_save_state_snapshot_dual_success(mock_file, mock_mkdir, isolated_monitor):
    """Ensure state snapshots are serialized to disk properly."""
    state_data = {
        "type": "LOCAL_STATE",
        "logic_date": "2026-04-08",
        "current": {"start_time": datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc)},
    }

    isolated_monitor._save_state_snapshot_dual("TEST/LINE_1", state_data)

    # Verify disk interaction
    mock_mkdir.assert_called_once()
    mock_file.assert_called_once_with(
        isolated_monitor.LOCAL_STATE_DIR / "TEST_LINE_1_LOCAL.json",
        "w",
        encoding="utf-8",
    )

    # Check that it serialized without failing on datetime objects
    written_data = "".join(call.args[0] for call in mock_file().write.call_args_list)
    assert "2026-04-08" in written_data


@patch("alerts.production_monitor.Path.exists", return_value=True)
@patch("alerts.production_monitor.Path.glob")
@patch("alerts.production_monitor.open", new_callable=mock_open)
def test_restore_state_from_disk(
    mock_file, mock_glob, mock_exists, mock_minio, mock_mapper
):
    """Ensure state correctly rebuilds from local JSON files."""

    mock_file_path = MagicMock()
    mock_file_path.name = "MY_TOPIC_COUNTERS.json"
    mock_glob.return_value = [mock_file_path]

    mock_json_content = """
    {
        "type": "GLOBAL_COUNTERS",
        "counters": {"ART-001": 5},
        "created_utc": "2026-04-08T10:00:00+00:00",
        "stored_key": "MY/TOPIC_COUNTERS"
    }
    """
    mock_file().read.return_value = mock_json_content

    with patch("alerts.production_monitor.threading.Thread.start"):
        monitor = ProductionMonitor(
            minio_adapter=mock_minio, mapper=mock_mapper, alert_notifier=None
        )

    assert "MY/TOPIC_COUNTERS" in monitor.state_memory
    assert monitor.state_memory["MY/TOPIC_COUNTERS"]["counters"]["ART-001"] == 5


# === UTILITY TESTS ===


def test_to_spanish_time_conversion(isolated_monitor):
    """Verify recursive datetime serialization standardizes to Madrid time string."""
    utc_time = datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc)

    payload = {"timestamp": utc_time, "nested": [utc_time, "string_val"]}

    result = isolated_monitor._to_spanish_time(payload)

    # 10:00 UTC = 12:00 CEST (Madrid in April)
    assert result["timestamp"] == "2026-04-08 12:00:00"
    assert result["nested"][0] == "2026-04-08 12:00:00"
    assert result["nested"][1] == "string_val"


@patch("alerts.production_monitor.Path.exists", return_value=True)
@patch("alerts.production_monitor.Path.glob")
@patch("alerts.production_monitor.json.load")
@patch("alerts.production_monitor.open", new_callable=mock_open)
def test_restore_state_from_disk_all_types(
    mock_file, mock_json_load, mock_glob, mock_exists, mock_minio, mock_mapper
):
    """Ensure state correctly rebuilds all three file types (COUNTERS, GLOBAL, LOCAL) and handles disk errors."""
    # 1. Mock the file paths yielded by glob()
    mock_counters = MagicMock()
    mock_counters.name = "TOPIC_COUNTERS.json"
    mock_global = MagicMock()
    mock_global.name = "TOPIC_GLOBAL.json"
    mock_local = MagicMock()
    mock_local.name = "TOPIC_DEVICE_LOCAL.json"
    mock_invalid = MagicMock()
    mock_invalid.name = "INVALID.json"
    mock_not_dict = MagicMock()
    mock_not_dict.name = "LIST.json"

    mock_glob.return_value = [
        mock_counters,
        mock_global,
        mock_local,
        mock_invalid,
        mock_not_dict,
    ]

    # 2. Mock the JSON payloads returned when each file is read
    mock_json_load.side_effect = [
        # 1. Counters payload
        {
            "type": "GLOBAL_COUNTERS",
            "counters": {"ART-001": 5},
            "stored_key": "MY/TOPIC_COUNTERS",
        },
        # 2. Global History payload
        {
            "topic": "MY/TOPIC",
            "date": "2026-04-08",
            "productions_completed": [{"start_time": "2026-04-08 10:00:00"}],
        },
        # 3. Local State payload (simulate stale date to trigger startup stop-alert override)
        {
            "stored_topic_key": "MY/TOPIC/DEVICE_1",
            "logic_date": "2025-01-01",
            "current": {"start_time": "2025-01-01 10:00:00"},
        },
        # 4. Exception case (Simulates a corrupted JSON file)
        Exception("Corrupted file"),
        # 5. Not a dict (Simulates a valid JSON file that contains a list instead of an object)
        ["List item"],
    ]

    with patch("alerts.production_monitor.threading.Thread.start"):
        monitor = ProductionMonitor(
            minio_adapter=mock_minio, mapper=mock_mapper, alert_notifier=None
        )

    # Assert COUNTERS loaded properly
    assert monitor.state_memory["MY/TOPIC_COUNTERS"]["counters"]["ART-001"] == 5

    # Assert GLOBAL loaded properly
    assert monitor.state_memory["MY/TOPIC"]["type"] == "SHARED_HISTORY"
    assert len(monitor.state_memory["MY/TOPIC"]["daily_history"]) == 1

    # Assert LOCAL loaded & Stale Logic was overridden
    local_state = monitor.state_memory["MY/TOPIC/DEVICE_1"]
    assert local_state["type"] == "LOCAL_STATE"
    assert (
        local_state["stop_alert_sent"] is True
    )  # Forced True because '2025-01-01' is stale!


# === UTILITY & PARSING TESTS ===


def test_parse_timestamps_utility(isolated_monitor):
    """Coverage for _parse_timestamps error handling and timezone localization."""
    # Valid dict with naive and aware timestamps
    data = {
        "start_time": "2026-04-08 10:00:00",
        "end_time": "2026-04-08T12:00:00+02:00",
    }
    parsed = isolated_monitor._parse_timestamps(data)
    assert parsed["start_time"].tzinfo is not None
    assert parsed["end_time"].tzinfo is not None

    # Invalid overall structures
    assert isolated_monitor._parse_timestamps(None) is None
    assert isolated_monitor._parse_timestamps(["not a dict"]) == ["not a dict"]

    # Bad timestamp string gracefully degrades to None instead of crashing
    bad_data = {"start_time": "INVALID_TIME_STRING"}
    parsed_bad = isolated_monitor._parse_timestamps(bad_data)
    assert parsed_bad["start_time"] is None


def test_detect_device_column(isolated_monitor):
    """Coverage for _detect_device_column fallback hierarchies."""
    # Success case
    df_valid = pd.DataFrame({"device_name": ["Device_A"]})
    assert isolated_monitor._detect_device_column(df_valid) == "device_name"

    # Failure case (columns missing, or columns exist but are all null)
    df_invalid = pd.DataFrame({"unknown_col": [1, 2], "linea": [None, None]})
    assert isolated_monitor._detect_device_column(df_invalid) is None


class ExplodingDatetime(datetime):
    """Custom datetime that throws an exception when timezone conversion is attempted."""

    def astimezone(self, tz=None):
        raise ValueError("Simulated timezone failure")


def test_to_spanish_time_fallback(isolated_monitor):
    """Coverage for datetime formatting exceptions inside _to_spanish_time."""
    dt = ExplodingDatetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc)

    # If astimezone fails, it should catch the exception and fall back to .isoformat()
    res = isolated_monitor._to_spanish_time(dt)

    assert "2026-04-08T10:00:00+00:00" in res
