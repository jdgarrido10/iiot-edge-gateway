"""
Test suite for the Production Alert System: async queues, SMTP dispatching, and HTML rendering.
"""

import pytest
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from alerts.production_alert import AlertNotifier
from config import Config

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def mock_config(monkeypatch: pytest.MonkeyPatch):
    """Isolate configuration to prevent real SMTP network calls."""
    monkeypatch.setattr(Config, "SMTP_HOST", "mock-smtp.local")
    monkeypatch.setattr(Config, "SMTP_PORT", 587)
    monkeypatch.setattr(Config, "SMTP_USER", "alert@mock.local")
    monkeypatch.setattr(Config, "SMTP_PASSWORD", "mock_secret")
    monkeypatch.setattr(Config, "SMTP_FROM_NAME", "Mock Notifier")
    monkeypatch.setattr(Config, "FORMS_STOP", "http://mock-forms.local/stop")
    monkeypatch.setattr(Config, "FORMS_RETURN", "http://mock-forms.local/return")


@pytest.fixture
def contacts_file(tmp_path) -> str:
    """Provide a temporary JSON file with various contact schema structures."""
    file_path = tmp_path / "test_contacts.json"
    data = {
        "default": {
            "recipients": [{"email": "default@mock.local", "manager": "Default Boss"}]
        },
        "topics": {
            "factory/line_1": {
                "recipients": [{"email": "line1@mock.local", "manager": "Alice"}]
            },
            "factory/legacy": {"email": "legacy@mock.local"},  # Legacy fallback format
        },
    }
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return str(file_path)


@pytest.fixture
def notifier(
    mock_config, contacts_file, monkeypatch: pytest.MonkeyPatch
) -> AlertNotifier:
    """Instantiate the AlertNotifier with a disabled background thread for synchronous testing."""
    # Prevent the background thread from starting automatically
    monkeypatch.setattr("alerts.production_alert.threading.Thread.start", MagicMock())

    app = AlertNotifier(contacts_file=contacts_file)
    yield app
    app.running = False


# =====================================================================
# Configuration & Contact Tests
# =====================================================================


def test_load_contacts_resolves_correctly(notifier: AlertNotifier):
    """Verify that specific topics resolve to their configured contacts, falling back to default."""
    config_l1 = notifier.get_contact_config("factory/line_1")
    config_unknown = notifier.get_contact_config("factory/unknown_line")

    assert config_l1["recipients"][0]["email"] == "line1@mock.local"
    assert config_unknown["recipients"][0]["email"] == "default@mock.local"


def test_load_contacts_handles_missing_file(tmp_path):
    """Ensure the system initializes safely even if the contacts JSON is missing."""
    missing_path = str(tmp_path / "non_existent.json")
    app = AlertNotifier(contacts_file=missing_path)

    assert app.contacts == {}
    assert app.default_contact == {}


# =====================================================================
# Producer / Queue Tests
# =====================================================================


def test_send_alert_enqueues_package(notifier: AlertNotifier):
    """Verify that send_alert successfully builds and enqueues the payload without blocking."""
    notifier.send_alert(
        topic="factory/test",
        message="Critical Failure",
        alert_type="STOP",
        data={"product": "Alpha"},
    )

    assert notifier._mailbox.qsize() == 1

    pkg = notifier._mailbox.get_nowait()
    assert pkg["topic"] == "factory/test"
    assert pkg["alert_type"] == "STOP"
    assert pkg["data"]["product"] == "Alpha"
    assert isinstance(pkg["timestamp"], datetime)


# =====================================================================
# Consumer / SMTP Dispatch Tests
# =====================================================================


@patch.object(AlertNotifier, "_send_smtp_bulk")
def test_process_dispatch_resolves_recipients(mock_send, notifier: AlertNotifier):
    """Verify standard recipient lists are parsed and passed to the SMTP bulk sender."""
    pkg = {
        "topic": "factory/line_1",
        "message": "x",
        "alert_type": "y",
        "timestamp": datetime.now(),
        "data": None,
    }

    notifier._process_dispatch(pkg)

    mock_send.assert_called_once()
    recipients = mock_send.call_args.kwargs["recipients"]
    assert recipients[0]["email"] == "line1@mock.local"


@patch.object(AlertNotifier, "_send_smtp_bulk")
def test_process_dispatch_supports_legacy_format(mock_send, notifier: AlertNotifier):
    """Ensure backwards compatibility with older, flat-dictionary contact formats."""
    pkg = {
        "topic": "factory/legacy",
        "message": "x",
        "alert_type": "y",
        "timestamp": datetime.now(),
        "data": None,
    }

    notifier._process_dispatch(pkg)

    mock_send.assert_called_once()
    recipients = mock_send.call_args.kwargs["recipients"]
    assert recipients[0]["email"] == "legacy@mock.local"


@patch("alerts.production_alert.smtplib.SMTP")
def test_send_smtp_bulk_success(mock_smtp_class, notifier: AlertNotifier, monkeypatch):
    """Verify the SMTP transaction logic: connection, TLS, auth, and loop dispatch."""
    # Prevent sleep delays during SMTP loop
    monkeypatch.setattr("alerts.production_alert.time.sleep", MagicMock())

    mock_server = MagicMock()
    mock_smtp_class.return_value.__enter__.return_value = mock_server

    recipients = [{"email": "target1@mock.local"}, {"email": "target2@mock.local"}]

    notifier._send_smtp_bulk(
        recipients=recipients,
        subject="Test Alert",
        message="Body",
        alert_type="STOP",
        timestamp=datetime.now(),
        topic="factory/test",
        alert_data=None,
    )

    mock_server.starttls.assert_called_once()
    mock_server.login.assert_called_once_with("alert@mock.local", "mock_secret")
    assert mock_server.send_message.call_count == 2


# =====================================================================
# Worker Loop Logic Tests
# =====================================================================


@patch.object(AlertNotifier, "_process_dispatch")
def test_mailbox_worker_consumes_queue(mock_dispatch, notifier: AlertNotifier):
    """Verify the daemon thread successfully drains the queue and processes messages."""
    notifier._mailbox.put({"topic": "test", "message": "payload"})

    # We use a side effect to kill the loop after it processes the first item
    def stop_loop(*args, **kwargs):
        notifier.running = False

    mock_dispatch.side_effect = stop_loop

    # Manually invoke the worker (which is safe because we didn't start the thread)
    notifier._mailbox_worker()

    mock_dispatch.assert_called_once()
    assert notifier._mailbox.qsize() == 0


# =====================================================================
# HTML Template Tests
# =====================================================================


def test_render_html_branching_stop_vs_return(notifier: AlertNotifier):
    """Verify the HTML template engine properly alters theme, text, and URLs based on event type."""
    dt = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    person = {"manager": "Alice"}

    # Test Stop Branch
    data_stop = {"product": "Alpha_Prod", "gap_seconds": 3600, "current_line": "LINE 1"}
    html_stop = notifier._render_html(
        "topic", person, "msg", "PARADA DETECTADA", dt, data_stop
    )

    assert "Justificar Parada" in html_stop
    assert "LÍNEA DETENIDA" in html_stop
    assert "Alpha_Prod" in html_stop
    assert "http://mock-forms.local/stop" in html_stop

    # Test Return Branch
    data_return = {"product": "Beta_Prod", "gap_seconds": 0, "current_line": "LINE 2"}
    html_return = notifier._render_html(
        "topic", person, "msg", "RETORNO DE MATERIAL", dt, data_return
    )

    assert "Justificar Incidencia" in html_return
    assert "REFERENCIA REPETIDA" in html_return
    assert "Beta_Prod" in html_return
    assert "http://mock-forms.local/return" in html_return
