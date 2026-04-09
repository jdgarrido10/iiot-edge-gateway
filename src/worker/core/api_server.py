"""
Lightweight HTTP API gateway for runtime control and health monitoring.
"""

import json
import os
import re
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

from config import Config
from core.logger import setup_logger

logger = setup_logger(__name__)

# Allowed CORS origins resolved once at import time
_ALLOWED_ORIGINS: list[str] = [
    url.strip()
    for url in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
]


class APIGatewayHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the pipeline control API.

    Endpoints
    ---------
    GET  /health   — Service liveness and dependency status.
    GET  /workers  — Current worker counts for all lines.
    POST /workers  — Update worker count for a single line.

    Dependencies (``mqtt_service``, ``influx_service``, ``worker_manager``)
    are injected via ``self.server.context`` at startup.
    """

    # ------------------------------------------------------------------
    # Response helpers
    # ------------------------------------------------------------------

    def _send_json(self, status: int, body: dict) -> None:
        """Write a JSON response with CORS headers."""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")

        origin = self.headers.get("Origin")
        if origin in _ALLOWED_ORIGINS:
            self.send_header("Access-Control-Allow-Origin", origin)

        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def _send_error(self, message: str, status: int = 500) -> None:
        self._send_json(status, {"error": message})

    # ------------------------------------------------------------------
    # HTTP verbs
    # ------------------------------------------------------------------

    def do_OPTIONS(self) -> None:
        """Handle CORS preflight requests."""
        self._send_json(200, {})

    def do_GET(self) -> None:
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/workers":
            self._handle_get_workers()
        else:
            self._send_error("Endpoint not found", status=404)

    def do_POST(self) -> None:
        if self.path == "/workers":
            self._handle_update_workers()
        else:
            self._send_error("Endpoint not found", status=404)

    # ------------------------------------------------------------------
    # Route handlers
    # ------------------------------------------------------------------

    def _handle_health(self) -> None:
        """Return liveness status of MQTT and InfluxDB connections."""
        try:
            mqtt_svc = self.server.context.get("mqtt_service")
            influx_svc = self.server.context.get("influx_service")

            mqtt_ok = False
            if mqtt_svc and hasattr(mqtt_svc, "client"):
                try:
                    mqtt_ok = mqtt_svc.client.is_connected()
                except Exception:
                    pass

            influx_ok = False
            if influx_svc and hasattr(influx_svc, "client"):
                try:
                    influx_ok = influx_svc.client.ping()
                except Exception:
                    pass

            overall = "healthy" if (mqtt_ok and influx_ok) else "degraded"
            self._send_json(
                200,
                {
                    "status": overall,
                    "mqtt": mqtt_ok,
                    "influx": influx_ok,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )

        except Exception as e:
            self._send_error(f"Health check failed: {e}")

    def _handle_get_workers(self) -> None:
        """Return current worker configuration for all production lines."""
        manager = self.server.context.get("worker_manager")
        if not manager:
            self._send_error("WorkerManager not available", status=503)
            return

        try:
            self._send_json(200, manager.get_all_workers())
        except Exception as e:
            self._send_error(str(e))

    def _handle_update_workers(self) -> None:
        """Update worker count for a single production line.

        Expected JSON body::

            {"line_id": "Linea_0202", "workers": 4}
        """
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_length).decode("utf-8"))

            if "line_id" not in body or "workers" not in body:
                self._send_error(
                    "Missing required fields: line_id, workers", status=400
                )
                return

            line_id = str(body["line_id"])
            if not re.match(r"^[a-zA-Z0-9_-]{1,50}$", line_id):
                self._send_error(
                    "Invalid line_id (alphanumeric, max 50 chars)", status=400
                )
                return

            manager = self.server.context.get("worker_manager")
            if not manager:
                self._send_error("WorkerManager not available", status=503)
                return

            success = manager.update_worker_count(line_id, int(body["workers"]))

            if success:
                self._send_json(
                    200,
                    {"success": True, "line_id": line_id, "workers": body["workers"]},
                )
            else:
                self._send_error("Failed to persist worker configuration")

        except Exception as e:
            self._send_error(str(e))

    def log_message(self, format: str, *args) -> None:
        """Suppress access logs unless DEBUG level is active."""
        if Config.LOG_LEVEL == "DEBUG":
            logger.debug(f"HTTP {format % args}")


# ------------------------------------------------------------------
# Server
# ------------------------------------------------------------------


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Multi-threaded HTTP server that does not block the ETL event loop."""

    daemon_threads = True


class APIServer:
    """Wrapper that manages the lifecycle of :class:`ThreadedHTTPServer`.

    Dependencies are injected at construction time and forwarded to handlers
    via ``server.context``.

    Args:
        port: TCP port to listen on.
        worker_manager: :class:`WorkerManager` instance.
        mqtt_service: MQTT service instance (must expose ``client.is_connected()``).
        influx_service: InfluxDB service instance (must expose ``client.ping()``).
    """

    def __init__(
        self,
        port: int = 8765,
        worker_manager=None,
        mqtt_service=None,
        influx_service=None,
    ):
        self.port = port
        self.server: ThreadedHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.context = {
            "worker_manager": worker_manager,
            "mqtt_service": mqtt_service,
            "influx_service": influx_service,
        }

    def start(self) -> None:
        """Bind the server and start serving in a daemon thread."""
        try:
            self.server = ThreadedHTTPServer(("0.0.0.0", self.port), APIGatewayHandler)
            self.server.context = self.context
            self._thread = threading.Thread(
                target=self.server.serve_forever, daemon=True
            )
            self._thread.start()
            logger.info(f"API server listening on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start API server: {e}")

    def stop(self) -> None:
        """Shut down the server and wait for the serving thread to finish."""
        if self.server:
            logger.info("Stopping API server...")
            self.server.shutdown()
            self.server.server_close()
            if self._thread:
                self._thread.join(timeout=5)
