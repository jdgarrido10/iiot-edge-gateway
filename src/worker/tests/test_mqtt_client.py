"""
Test suite for the Message Broker client: connection lifecycle, callbacks, and backoff resilience.
"""

import pytest
from unittest.mock import MagicMock, patch
import time

from adapters.mqtt_client import MQTTService
from config import Config

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def mock_config(monkeypatch: pytest.MonkeyPatch):
    """Isolate configuration to prevent real external network calls."""
    monkeypatch.setattr(Config, "MQTT_BROKER", "mock-broker.local")
    monkeypatch.setattr(Config, "MQTT_PORT", 1883)
    monkeypatch.setattr(Config, "MQTT_TOPIC", "factory/mock_topic")
    monkeypatch.setattr(Config, "MQTT_USER", "mock_user")
    monkeypatch.setattr(Config, "MQTT_PASSWORD", "mock_pass")


@pytest.fixture
def mock_paho_client():
    """Mock the underlying Paho MQTT client."""
    with patch("adapters.mqtt_client.mqtt.Client") as mock_client_class:
        mock_instance = MagicMock()
        mock_client_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_sleep(monkeypatch: pytest.MonkeyPatch):
    """Mock time.sleep to prevent tests from hanging during exponential backoff logic."""
    sleep_mock = MagicMock()
    monkeypatch.setattr(time, "sleep", sleep_mock)
    return sleep_mock


@pytest.fixture
def dummy_callback():
    """Provide a minimal callback function to satisfy the service signature."""

    def callback(client, userdata, message):
        pass

    return callback


@pytest.fixture
def mqtt_service(mock_config, mock_paho_client, dummy_callback) -> MQTTService:
    """Instantiate the MQTTService with mocked dependencies."""
    return MQTTService(on_message_callback=dummy_callback)


# =====================================================================
# Initialization & Setup Tests
# =====================================================================


def test_initialization_sets_credentials_and_callbacks(
    mock_config, mock_paho_client, dummy_callback
):
    """Verify that the client initializes correctly with credentials and assigns callbacks."""
    service = MQTTService(on_message_callback=dummy_callback)

    mock_paho_client.username_pw_set.assert_called_once_with("mock_user", "mock_pass")

    assert service.client.on_connect == service._on_connect
    assert service.client.on_disconnect == service._on_disconnect
    assert service.client.on_message == dummy_callback


def test_initialization_without_credentials(
    monkeypatch: pytest.MonkeyPatch, mock_paho_client, dummy_callback
):
    """Ensure the client can initialize gracefully even in anonymous network environments."""
    monkeypatch.setattr(Config, "MQTT_USER", None)
    monkeypatch.setattr(Config, "MQTT_PASSWORD", None)

    MQTTService(on_message_callback=dummy_callback)

    mock_paho_client.username_pw_set.assert_not_called()


# =====================================================================
# Callback Logic Tests
# =====================================================================


def test_on_connect_success_triggers_subscription(
    mqtt_service: MQTTService, mock_paho_client
):
    """Ensure that a successful connection (rc=0) automatically subscribes to the target topic."""
    mqtt_service._on_connect(client=mock_paho_client, userdata=None, flags=None, rc=0)

    mock_paho_client.subscribe.assert_called_once_with("factory/mock_topic")


def test_on_connect_refused_skips_subscription(
    mqtt_service: MQTTService, mock_paho_client
):
    """Ensure that a refused connection (rc!=0) does not attempt to subscribe."""
    # Simulate a connection refusal (e.g., bad credentials)
    mqtt_service._on_connect(client=mock_paho_client, userdata=None, flags=None, rc=5)

    mock_paho_client.subscribe.assert_not_called()


@patch("adapters.mqtt_client.logger")
def test_on_disconnect_logs_unexpected_events(
    mock_logger, mqtt_service: MQTTService, mock_paho_client
):
    """Verify that unexpected disconnections trigger proper warnings for debugging."""
    # Simulate an unexpected disconnection
    mqtt_service._on_disconnect(client=mock_paho_client, userdata=None, rc=1)

    # Verify the internal logger caught the event
    mock_logger.warning.assert_called_once()
    log_message = mock_logger.warning.call_args[0][0]
    assert "Unexpected MQTT disconnect" in log_message


# =====================================================================
# Network Connection & Resilience Tests
# =====================================================================


def test_connect_and_loop_immediate_success(
    mqtt_service: MQTTService, mock_paho_client, mock_sleep
):
    """Standard happy path: immediate connection without retries."""
    success = mqtt_service.connect_and_loop()

    assert success is True
    mock_paho_client.connect.assert_called_once_with(
        "mock-broker.local", 1883, keepalive=60
    )
    mock_paho_client.loop_start.assert_called_once()
    mock_sleep.assert_not_called()


def test_connect_and_loop_recovers_after_retries(
    mqtt_service: MQTTService, mock_paho_client, mock_sleep
):
    """Resilience test: verify the exponential backoff correctly delays and eventually recovers."""
    # Simulate 2 network failures followed by a success
    mock_paho_client.connect.side_effect = [
        Exception("Network unreachable"),
        Exception("Connection refused"),
        None,
    ]

    success = mqtt_service.connect_and_loop()

    assert success is True
    assert mock_paho_client.connect.call_count == 3
    mock_paho_client.loop_start.assert_called_once()

    # Verify exponential backoff: 2^0 = 1s, 2^1 = 2s
    mock_sleep.assert_any_call(1)
    mock_sleep.assert_any_call(2)
    assert mock_sleep.call_count == 2


def test_connect_and_loop_exhausts_retries(
    mqtt_service: MQTTService, mock_paho_client, mock_sleep
):
    """Resilience test: ensure the system safely aborts after exceeding MAX_RETRIES."""
    # Simulate permanent network failure
    mock_paho_client.connect.side_effect = Exception("Host down")

    success = mqtt_service.connect_and_loop()

    assert success is False
    assert mock_paho_client.connect.call_count == mqtt_service._MAX_RETRIES
    mock_paho_client.loop_start.assert_not_called()
    assert mock_sleep.call_count == mqtt_service._MAX_RETRIES


# =====================================================================
# Teardown Tests
# =====================================================================


def test_stop_terminates_loop(mqtt_service: MQTTService, mock_paho_client):
    """Ensure the networking background thread is properly terminated on shutdown."""
    mqtt_service.stop()
    mock_paho_client.loop_stop.assert_called_once()
