"""
Test suite for the API Gateway: routing, health checks, worker configurations, and server lifecycle.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from core.api_server import APIGatewayHandler, APIServer

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def mock_context():
    """Provides mocked dependencies to simulate the application context."""
    mqtt_mock = MagicMock()
    mqtt_mock.client.is_connected.return_value = True

    influx_mock = MagicMock()
    influx_mock.client.ping.return_value = True

    worker_manager_mock = MagicMock()
    worker_manager_mock.get_all_workers.return_value = {"Line_1": 4, "Line_2": 2}
    worker_manager_mock.update_worker_count.return_value = True

    return {
        "mqtt_service": mqtt_mock,
        "influx_service": influx_mock,
        "worker_manager": worker_manager_mock,
    }


@pytest.fixture
def api_handler(mock_context):
    """
    Creates an APIGatewayHandler completely isolated from network sockets.
    Bypasses __init__ to prevent it from attempting to read a real socket stream.
    """
    handler = APIGatewayHandler.__new__(APIGatewayHandler)

    # Inject Server and Context
    handler.server = MagicMock()
    handler.server.context = mock_context

    # Mock HTTP streams and headers
    handler.wfile = MagicMock()
    handler.rfile = MagicMock()
    handler.headers = {"Origin": "http://localhost:3000"}
    handler.path = "/"

    # Mock internal response methods to prevent actual socket writes
    handler.send_response = MagicMock()
    handler.send_header = MagicMock()
    handler.end_headers = MagicMock()

    return handler


# =====================================================================
# Routing & Base HTTP Tests
# =====================================================================


def test_cors_preflight_options(api_handler):
    """Ensure OPTIONS requests return a 200 OK for CORS preflight."""
    api_handler.do_OPTIONS()

    api_handler.send_response.assert_called_once_with(200)
    api_handler.send_header.assert_any_call(
        "Access-Control-Allow-Methods", "POST, GET, OPTIONS"
    )


def test_not_found_routes(api_handler):
    """Ensure invalid paths return 404 for both GET and POST."""
    api_handler.path = "/unknown-endpoint"

    api_handler.do_GET()
    api_handler.send_response.assert_called_with(404)

    api_handler.do_POST()
    api_handler.send_response.assert_called_with(404)


# =====================================================================
# GET /health Tests
# =====================================================================


def test_health_check_healthy(api_handler):
    """Ensure the health endpoint returns 'healthy' when dependencies are active."""
    api_handler.path = "/health"
    api_handler.do_GET()

    api_handler.send_response.assert_called_with(200)

    # Extract the written JSON payload
    written_data = api_handler.wfile.write.call_args[0][0].decode("utf-8")
    payload = json.loads(written_data)

    assert payload["status"] == "healthy"
    assert payload["mqtt"] is True
    assert payload["influx"] is True


def test_health_check_degraded_on_exception(api_handler, mock_context):
    """Ensure health gracefully reports 'degraded' if a dependency throws an exception."""
    # Force the message broker mock to raise an error
    mock_context["mqtt_service"].client.is_connected.side_effect = Exception(
        "Connection lost"
    )

    api_handler.path = "/health"
    api_handler.do_GET()

    api_handler.send_response.assert_called_with(200)
    written_data = api_handler.wfile.write.call_args[0][0].decode("utf-8")
    payload = json.loads(written_data)

    assert payload["status"] == "degraded"
    assert payload["mqtt"] is False


# =====================================================================
# GET /workers Tests
# =====================================================================


def test_get_workers_success(api_handler):
    """Ensure worker configurations are correctly fetched and returned."""
    api_handler.path = "/workers"
    api_handler.do_GET()

    api_handler.send_response.assert_called_with(200)
    written_data = api_handler.wfile.write.call_args[0][0].decode("utf-8")
    payload = json.loads(written_data)

    assert payload["Line_1"] == 4
    assert payload["Line_2"] == 2


def test_get_workers_missing_manager(api_handler, mock_context):
    """Ensure a 503 is returned if the WorkerManager is not injected."""
    mock_context["worker_manager"] = None

    api_handler.path = "/workers"
    api_handler.do_GET()

    api_handler.send_response.assert_called_with(503)


# =====================================================================
# POST /workers Tests
# =====================================================================


def test_post_workers_success(api_handler, mock_context):
    """Ensure valid worker updates are processed successfully."""
    body = json.dumps({"line_id": "Line_1", "workers": 5}).encode("utf-8")
    api_handler.headers["Content-Length"] = str(len(body))
    api_handler.rfile.read.return_value = body

    api_handler.path = "/workers"
    api_handler.do_POST()

    api_handler.send_response.assert_called_with(200)
    mock_context["worker_manager"].update_worker_count.assert_called_once_with(
        "Line_1", 5
    )


def test_post_workers_missing_fields(api_handler):
    """Ensure 400 is returned if required JSON fields are missing."""
    body = json.dumps({"line_id": "Line_1"}).encode("utf-8")  # Missing 'workers'
    api_handler.headers["Content-Length"] = str(len(body))
    api_handler.rfile.read.return_value = body

    api_handler.path = "/workers"
    api_handler.do_POST()

    api_handler.send_response.assert_called_with(400)


def test_post_workers_invalid_line_id(api_handler):
    """Ensure 400 is returned if the line_id fails regex validation."""
    body = json.dumps({"line_id": "Line@123!!", "workers": 2}).encode("utf-8")
    api_handler.headers["Content-Length"] = str(len(body))
    api_handler.rfile.read.return_value = body

    api_handler.path = "/workers"
    api_handler.do_POST()

    api_handler.send_response.assert_called_with(400)


def test_post_workers_update_failed(api_handler, mock_context):
    """Ensure 500 is returned if the manager fails to persist the update."""
    mock_context["worker_manager"].update_worker_count.return_value = False

    body = json.dumps({"line_id": "Line_1", "workers": 5}).encode("utf-8")
    api_handler.headers["Content-Length"] = str(len(body))
    api_handler.rfile.read.return_value = body

    api_handler.path = "/workers"
    api_handler.do_POST()

    api_handler.send_response.assert_called_with(500)


# =====================================================================
# Logging Tests
# =====================================================================


@patch("core.api_server.logger")
@patch("core.api_server.Config")
def test_log_message_debug_mode(mock_config, mock_logger, api_handler):
    """Ensure HTTP logs are only emitted when DEBUG level is active."""
    mock_config.LOG_LEVEL = "DEBUG"
    api_handler.log_message("Request from %s", "127.0.0.1")

    mock_logger.debug.assert_called_once_with("HTTP Request from 127.0.0.1")


@patch("core.api_server.logger")
@patch("core.api_server.Config")
def test_log_message_suppressed(mock_config, mock_logger, api_handler):
    """Ensure HTTP access logs are silenced in production to avoid noise."""
    mock_config.LOG_LEVEL = "INFO"
    api_handler.log_message("Request from %s", "127.0.0.1")

    mock_logger.debug.assert_not_called()


# =====================================================================
# Server Lifecycle Tests
# =====================================================================


@patch("core.api_server.ThreadedHTTPServer")
@patch("core.api_server.threading.Thread")
def test_api_server_lifecycle(mock_thread_class, mock_http_server_class):
    """Verify that the APIServer safely spins up and tears down its background thread."""
    mock_server_instance = MagicMock()
    mock_http_server_class.return_value = mock_server_instance

    mock_thread_instance = MagicMock()
    mock_thread_class.return_value = mock_thread_instance

    server = APIServer(port=8080)

    # Test Start
    server.start()
    mock_http_server_class.assert_called_once()
    mock_thread_instance.start.assert_called_once()

    # Test Stop
    server.stop()
    mock_server_instance.shutdown.assert_called_once()
    mock_server_instance.server_close.assert_called_once()
    mock_thread_instance.join.assert_called_once()


@patch("core.api_server.ThreadedHTTPServer")
@patch("core.api_server.logger")
def test_api_server_start_exception(mock_logger, mock_http_server_class):
    """Ensure the system does not crash if the port is already bound."""
    mock_http_server_class.side_effect = OSError("Address already in use")

    server = APIServer(port=8080)
    server.start()  # Should handle the exception gracefully

    mock_logger.error.assert_called_once()


def test_api_server_stop_before_start():
    """Ensure calling stop() on an uninitialized server does not raise an error."""
    server = APIServer(port=8080)
    server.stop()  # Should safely execute without NullPointer exceptions
