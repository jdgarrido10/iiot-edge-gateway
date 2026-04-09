"""
MQTT client with exponential-backoff reconnection logic.
"""

import time

import paho.mqtt.client as mqtt

from config import Config
from core.logger import setup_logger

logger = setup_logger(__name__)


class MQTTService:
    """Thin wrapper around :class:`paho.mqtt.client.Client` with auto-reconnect.

    Re-subscription is handled automatically in :meth:`_on_connect`, which is
    triggered by paho's internal reconnection loop whenever the connection is
    re-established after an unexpected disconnect.

    Args:
        on_message_callback: Callable passed directly to ``client.on_message``.
            Must have the signature ``(client, userdata, message)``.
    """

    _MAX_RETRIES = 12

    def __init__(self, on_message_callback):
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = on_message_callback

        if Config.MQTT_USER and Config.MQTT_PASSWORD:
            self.client.username_pw_set(Config.MQTT_USER, Config.MQTT_PASSWORD)

    # ------------------------------------------------------------------
    # Paho callbacks
    # ------------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, rc: int) -> None:
        """Subscribe to the configured topic on successful connection."""
        if rc == 0:
            logger.info(f"MQTT connected — subscribing to {Config.MQTT_TOPIC}")
            client.subscribe(Config.MQTT_TOPIC)
        else:
            logger.warning(f"MQTT connection refused (rc={rc})")

    def _on_disconnect(self, client, userdata, rc: int) -> None:
        """Log unexpected disconnections.

        Paho's ``loop_start()`` handles automatic reconnection; re-subscription
        occurs in :meth:`_on_connect` once the connection is re-established.
        """
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnect (rc={rc}) — reconnecting...")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect_and_loop(self) -> bool:
        """Connect to the broker and start the background network loop.

        Retries up to ``_MAX_RETRIES`` times using exponential backoff
        capped at 30 seconds.

        Returns:
            True on success, False if all retry attempts are exhausted.
        """
        for attempt in range(self._MAX_RETRIES):
            try:
                logger.info(f"Connecting to MQTT broker at {Config.MQTT_BROKER}...")
                self.client.connect(Config.MQTT_BROKER, Config.MQTT_PORT, keepalive=60)
                self.client.loop_start()
                return True
            except Exception as e:
                delay = min(2**attempt, 30)
                logger.warning(
                    f"MQTT connection attempt {attempt + 1}/{self._MAX_RETRIES} failed: {e} "
                    f"— retrying in {delay}s"
                )
                time.sleep(delay)

        logger.error(f"MQTT connection failed after {self._MAX_RETRIES} attempts")
        return False

    def stop(self) -> None:
        """Stop the background network loop."""
        self.client.loop_stop()
