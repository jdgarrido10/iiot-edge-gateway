"""
Persistent state manager with atomic checkpointing for cross-restart deduplication.
"""

import json
import os
import threading
import pandas as pd
import tempfile
from datetime import datetime

from config import Config
from core.logger import setup_logger

logger = setup_logger(__name__)


class StateManager:
    """Tracks the last processed record per MQTT topic across process restarts.

    Durability is achieved via atomic checkpoint writes (tempfile + ``os.replace``).
    Deduplication is identity-based: records with an ``identity`` value equal to
    or below the saved value are considered already processed, with rollover
    detection to handle counter resets.

    WAL-based in-flight recovery is intentionally omitted. The ETL is idempotent
    and paho-mqtt re-delivers unacknowledged messages on reconnect, making
    in-flight batch recovery unnecessary. The :meth:`recover_pending_batches`
    method is retained as a documented no-op for API compatibility.
    """

    def __init__(self):
        self._lock = threading.Lock()
        Config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def recover_pending_batches(self, buffer_manager) -> None:
        """No-op stub retained for API compatibility.

        WAL-based recovery is not implemented. If re-introduced, inject
        the :class:`BufferManager` here to re-enqueue in-flight items.
        """
        logger.debug("recover_pending_batches: WAL recovery disabled — skipping")

    # ------------------------------------------------------------------
    # Checkpoint I/O
    # ------------------------------------------------------------------

    def _load_state(self) -> dict:
        """Load state from the checkpoint file on disk.

        Returns an empty dict if no checkpoint exists or if the file is
        corrupted (the corrupted file is renamed for post-mortem analysis).
        """
        if not Config.CHECKPOINT_FILE.exists():
            logger.info("No checkpoint found — starting with clean state")
            return {}

        try:
            with open(Config.CHECKPOINT_FILE) as f:
                state = json.load(f)
            logger.info(f"Checkpoint loaded: {len(state)} topic(s)")
            return state

        except json.JSONDecodeError as e:
            logger.error(f"Corrupted checkpoint: {e} — starting clean")
            backup = Config.CHECKPOINT_FILE.with_suffix(".json.corrupted")
            Config.CHECKPOINT_FILE.rename(backup)
            return {}

        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}", exc_info=True)
            return {}

    def save_checkpoint(self, df: "pd.DataFrame", topic: str) -> None:
        """Atomically persist the last processed record identity for a topic.

        Args:
            df: Processed DataFrame; the last row is used as the checkpoint.
            topic: MQTT topic the data originated from.
        """
        if df is None or df.empty:
            return

        try:
            last = df.iloc[-1]
            identity_val = int(last.get("identity") or last.get("Identity") or 0)
            uuid_val = str(last.get("record_uuid") or last.get("Id") or "unknown")
            ts_val = str(
                last.get("timestamp")
                or last.get("Timestamp")
                or datetime.now().isoformat()
            )

            entry = {
                "Timestamp": ts_val,
                "Id": uuid_val,
                "Identity": identity_val,
                "saved_at": datetime.now().isoformat(),
                "record_count": len(df),
            }

            with self._lock:
                self.state[topic] = entry
                self._write_checkpoint_atomic(self.state)

            logger.debug(f"Checkpoint saved: {topic} (identity={identity_val})")

        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}", exc_info=True)

    def _write_checkpoint_atomic(self, state_data: dict) -> None:
        """Write checkpoint using tempfile + ``os.replace`` for atomicity.

        Must be called with ``_lock`` already held.
        """
        tmp_name = None
        try:
            dirname = Config.CHECKPOINT_FILE.parent
            with tempfile.NamedTemporaryFile(
                mode="w",
                dir=dirname,
                delete=False,
                prefix=".checkpoint_",
                suffix=".tmp",
            ) as tmp_file:
                json.dump(state_data, tmp_file, indent=4, ensure_ascii=False)
                tmp_name = tmp_file.name

            os.replace(tmp_name, Config.CHECKPOINT_FILE)

        except Exception as e:
            logger.error(f"Atomic checkpoint write failed: {e}", exc_info=True)
            if tmp_name and os.path.exists(tmp_name):
                try:
                    os.remove(tmp_name)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def is_already_processed(self, identity, topic: str) -> bool:
        """Return True if this identity has already been processed for the topic.

        Counter rollover is handled: when the gap between the stored identity
        and the current one exceeds ``Config.ROLLOVER_THRESHOLD``, the record
        is treated as new (counter has wrapped around).

        Args:
            identity: Identity counter value from the incoming record.
            topic: MQTT topic the record arrived on.

        Returns:
            True if the record is a duplicate and should be skipped.
        """
        with self._lock:
            if topic not in self.state:
                return False
            last_identity = int(self.state[topic].get("Identity", 0))

        current = int(identity)

        if current > last_identity:
            return False

        if (last_identity - current) > Config.ROLLOVER_THRESHOLD:
            logger.debug(f"Counter rollover on {topic}: {last_identity} → {current}")
            return False

        return True

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def get_state_summary(self) -> dict:
        """Return a monitoring-friendly summary of all tracked topics.

        Returns:
            Dict with ``topics_tracked`` count and per-topic metadata.
        """
        with self._lock:
            return {
                "topics_tracked": len(self.state),
                "topics": {
                    topic: {
                        "last_identity": info.get("Identity"),
                        "last_saved": info.get("saved_at"),
                        "record_count": info.get("record_count"),
                    }
                    for topic, info in self.state.items()
                },
            }
