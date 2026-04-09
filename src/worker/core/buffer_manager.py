"""
Thread-safe buffer manager with per-topic granular locks and Dead Letter Queue (DLQ).
"""

import time
import json
import threading
from datetime import datetime

from config import Config
from core.logger import setup_logger

logger = setup_logger(__name__)


class BufferManager:
    """Accumulate MQTT messages per topic and release them in batches.

    Each topic gets its own dedicated lock (granular locking) to maximise
    concurrency. A global lock is used only for the short critical section
    of creating new topic entries.

    Messages that fail JSON parsing, exceed the per-topic buffer size, or
    exhaust their retry budget are written to the Dead Letter Queue (DLQ)
    for offline analysis.

    Buffer entries have the shape::

        {
            "data": [{"payload": {...}, "attempts": int, "received_at": float}, ...],
            "last_time": float,    # epoch seconds of the last insert
            "lock": threading.Lock,
        }
    """

    def __init__(self):
        # topic → buffer entry dict (plain dict, not defaultdict, to control creation)
        self.buffers: dict = {}
        self._global_lock = threading.Lock()

        self._max_items = Config.MAX_BUFFER_SIZE
        self._max_retries = 3

        Config.DLQ_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_buffer_entry(self) -> dict:
        """Create a fresh buffer entry with its own dedicated lock."""
        return {
            "data": [],
            "last_time": time.time(),
            "lock": threading.Lock(),
        }

    def _get_or_create_entry(self, topic: str) -> dict:
        """Return the buffer entry for *topic*, creating it if necessary.

        The global lock is held only for the creation check, keeping the
        critical section as short as possible.
        """
        with self._global_lock:
            if topic not in self.buffers:
                self.buffers[topic] = self._make_buffer_entry()
            return self.buffers[topic]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_message(self, topic: str, payload_str: str) -> bool:
        """Parse and enqueue a raw JSON payload for the given topic.

        Adds the ``mqtt_topic`` field to each item if absent.

        Args:
            topic: MQTT topic the message arrived on.
            payload_str: Raw JSON string (object or array of objects).

        Returns:
            True when the buffer has reached ``Config.BATCH_SIZE`` and
            should be flushed immediately; False otherwise.
        """
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError as e:
            logger.error(
                f"Invalid JSON on topic '{topic}': {e}",
                extra={"topic": topic, "error": str(e)},
            )
            self._save_to_dlq(topic, payload_str, "json_decode_error")
            return False

        items = payload if isinstance(payload, list) else [payload]
        entry = self._get_or_create_entry(topic)

        with entry["lock"]:
            current_len = len(entry["data"])

            if current_len + len(items) > self._max_items:
                logger.warning(
                    f"Buffer overflow on topic '{topic}'",
                    extra={
                        "topic": topic,
                        "current": current_len,
                        "incoming": len(items),
                        "max": self._max_items,
                    },
                )
                self._save_to_dlq(topic, items, "buffer_overflow")
                return False

            for item in items:
                if isinstance(item, dict):
                    item.setdefault("mqtt_topic", topic)
                entry["data"].append(
                    {
                        "payload": item,
                        "attempts": 0,
                        "received_at": time.time(),
                    }
                )

            return len(entry["data"]) >= Config.BATCH_SIZE

    def get_ready_topics(self) -> list[str]:
        """Return a list of topics whose buffers are ready for processing.

        A topic is ready when either:
        - Its buffer has reached ``Config.BATCH_SIZE``, or
        - ``Config.BATCH_TIMEOUT`` seconds have elapsed since the last insert.

        Returns:
            List of topic strings ready for :meth:`pop_buffer`.
        """
        ready: list[str] = []
        now = time.time()

        with self._global_lock:
            topic_snapshot = list(self.buffers.keys())

        for topic in topic_snapshot:
            with self._global_lock:
                if topic not in self.buffers:
                    continue
                entry = self.buffers[topic]

            with entry["lock"]:
                size = len(entry["data"])
                if size == 0:
                    continue

                elapsed = now - entry["last_time"]
                if size >= Config.BATCH_SIZE or elapsed > Config.BATCH_TIMEOUT:
                    ready.append(topic)

        return ready

    def pop_buffer(self, topic: str) -> list:
        """Atomically extract and return all buffered items for a topic.

        The buffer is reset and the timer is restarted in the same lock
        acquisition, so no items can be lost between pop and re-insert.

        Args:
            topic: Topic to drain.

        Returns:
            List of wrapped message dicts, or an empty list if the topic
            has no data or does not exist.
        """
        with self._global_lock:
            if topic not in self.buffers:
                return []
            entry = self.buffers[topic]

        with entry["lock"]:
            data = entry["data"]
            if not data:
                return []
            entry["data"] = []
            entry["last_time"] = time.time()
            return data

    def restore_buffer(self, topic: str, wrapped_data: list) -> None:
        """Re-enqueue failed items according to the retry policy.

        Items that have not yet reached ``_max_retries`` are prepended to
        the buffer (preserving relative order). Items that have exhausted
        their retries are sent to the DLQ.

        Args:
            topic: Topic to restore items to.
            wrapped_data: List of wrapped message dicts from a failed batch.
        """
        retry_items: list = []
        dlq_items: list = []

        for item in wrapped_data:
            item["attempts"] += 1
            if item["attempts"] < self._max_retries:
                retry_items.append(item)
            else:
                dlq_items.append(item["payload"])

        if dlq_items:
            logger.warning(
                f"DLQ: discarding {len(dlq_items)} message(s) after {self._max_retries} failures",
                extra={"topic": topic, "count": len(dlq_items)},
            )
            self._save_to_dlq(topic, dlq_items, "max_retries_exceeded")

        if retry_items:
            entry = self._get_or_create_entry(topic)
            with entry["lock"]:
                entry["data"] = retry_items + entry["data"]

    def get_safe_keys(self) -> list[str]:
        """Return a snapshot of current topic keys safe for iteration."""
        with self._global_lock:
            return list(self.buffers.keys())

    def get_buffer_size(self, topic: str) -> int:
        """Return the current item count for a single topic.

        Args:
            topic: Topic to query.

        Returns:
            Item count, or 0 if the topic does not exist.
        """
        with self._global_lock:
            if topic not in self.buffers:
                return 0
            entry = self.buffers[topic]

        with entry["lock"]:
            return len(entry["data"])

    def get_all_buffer_sizes(self) -> dict[str, int]:
        """Return a dict mapping every tracked topic to its current buffer size."""
        with self._global_lock:
            topics = list(self.buffers.keys())

        return {topic: self.get_buffer_size(topic) for topic in topics}

    # ------------------------------------------------------------------
    # Dead Letter Queue
    # ------------------------------------------------------------------

    def _save_to_dlq(self, topic: str, data, reason: str) -> None:
        """Write rejected messages to the Dead Letter Queue directory.

        DLQ files are named ``{safe_topic}_{timestamp}_{reason}.json``
        and contain the raw payload along with metadata for debugging.

        Args:
            topic: Originating MQTT topic.
            data: Payload to archive (string, dict, or list).
            reason: Short reason code (e.g. ``"buffer_overflow"``).
        """
        try:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            safe_topic = topic.replace("/", "_")
            filename = f"{safe_topic}_{timestamp_str}_{reason}.json"
            filepath = Config.DLQ_DIR / filename

            entry = {
                "timestamp": datetime.now().isoformat(),
                "topic": topic,
                "reason": reason,
                "data": data,
            }

            with open(filepath, "w") as f:
                json.dump(entry, f, indent=2, ensure_ascii=False)

            logger.warning(f"DLQ: saved '{filename}'")

        except Exception as e:
            logger.error(f"Error writing to DLQ: {e}", exc_info=True)
