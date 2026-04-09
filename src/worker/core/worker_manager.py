"""
Per-line worker count manager with atomic persistence and async MinIO logging.
"""

import json
import os
import threading
import tempfile
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from config import Config
from core.logger import setup_logger

logger = setup_logger(__name__)

try:
    from . import etl_config
except ImportError:
    try:
        from config import etl_config
    except ImportError:

        class etl_config:  # type: ignore[no-redef]
            DEFAULT_WORKERS_PER_LINE: dict = {"default": 1}

        logger.warning("etl_config not found — using minimal defaults")


class WorkerManager:
    """Manage the number of active workers per production line.

    Worker counts are persisted atomically to a JSON config file so they
    survive process restarts. Updates are also logged asynchronously to
    MinIO when a ``minio_adapter`` is provided.

    Args:
        minio_adapter: Optional adapter for async MinIO logging of changes.
        config_path: Path to the JSON worker config file.
    """

    def __init__(
        self, minio_adapter=None, config_path: str = "/app/data/config_workers.json"
    ):
        self.config_path = config_path
        self._lock = threading.Lock()
        self.minio_adapter = minio_adapter
        self._io_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="MinioLogger"
        )

        self._validate_etl_config()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_etl_config(self) -> None:
        """Ensure ``etl_config`` has the required attributes with safe defaults."""
        if not hasattr(etl_config, "DEFAULT_WORKERS_PER_LINE"):
            etl_config.DEFAULT_WORKERS_PER_LINE = {"default": 1}
        etl_config.DEFAULT_WORKERS_PER_LINE.setdefault("default", 1)

    def _load_config(self) -> dict:
        """Read the worker config file safely (thread-safe).

        Returns:
            Dict mapping line_id → worker count, or an empty dict on error.
        """
        with self._lock:
            if not os.path.exists(self.config_path):
                return {}
            try:
                with open(self.config_path) as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error reading worker config: {e}")
                return {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update_worker_count(self, line_id: str, count: int) -> bool:
        """Set the active worker count for a production line.

        The update is written atomically via tempfile + ``os.replace``.
        If a ``minio_adapter`` is configured, the change is also logged
        asynchronously.

        Args:
            line_id: Production line identifier (e.g. ``"Linea_0202"``).
            count: New worker count. Must be in ``[0, Config.MAX_WORKERS_PER_LINE]``.

        Returns:
            True on success, False if the value is out of range or a write error occurs.
        """
        if not (0 <= count <= Config.MAX_WORKERS_PER_LINE):
            logger.warning(f"Invalid worker count for {line_id}: {count}")
            return False

        with self._lock:
            config: dict = {}
            if os.path.exists(self.config_path):
                try:
                    with open(self.config_path) as f:
                        content = f.read().strip()
                        if content:
                            config = json.loads(content)
                except Exception as e:
                    logger.error(f"Error loading worker config before update: {e}")

            config[line_id] = count

            try:
                dirname = os.path.dirname(self.config_path)
                os.makedirs(dirname, exist_ok=True)

                with tempfile.NamedTemporaryFile("w", dir=dirname, delete=False) as tf:
                    json.dump(config, tf, indent=2)
                    tmp_name = tf.name

                os.replace(tmp_name, self.config_path)
                logger.info(f"Worker count updated: {line_id}={count}")

            except Exception as e:
                logger.error(f"Error writing worker config: {e}")
                return False

        if self.minio_adapter:
            self._io_executor.submit(self.minio_adapter.save_worker_log, line_id, count)

        return True

    def get_all_workers(self) -> dict:
        """Return the current worker count for all configured lines.

        Returns:
            Dict mapping line_id → worker count.
        """
        return self._load_config()

    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add or update the ``active_workers`` column using the current config.

        Lookup is by the ``linea`` column. Lines not found in the config fall
        back to ``etl_config.DEFAULT_WORKERS_PER_LINE``, then to ``1``.
        Values exceeding ``Config.MAX_WORKERS_PER_LINE`` are clamped.

        Args:
            df: DataFrame with a ``linea`` column.

        Returns:
            DataFrame with ``active_workers`` populated.
        """
        if df.empty:
            return df

        workers_map = self._load_config()

        if "linea" in df.columns:
            clean_keys = df["linea"].astype(str).str.strip()
            df["active_workers"] = clean_keys.map(workers_map)

            mask_nan = df["active_workers"].isna()
            if mask_nan.any():
                defaults = etl_config.DEFAULT_WORKERS_PER_LINE
                df.loc[mask_nan, "active_workers"] = df.loc[mask_nan, "linea"].apply(
                    lambda v: defaults.get(str(v).strip(), defaults.get("default", 1))
                )

            df["active_workers"] = (
                pd.to_numeric(df["active_workers"], errors="coerce")
                .fillna(1)
                .astype(int)
            )
            df.loc[
                df["active_workers"] > Config.MAX_WORKERS_PER_LINE, "active_workers"
            ] = Config.MAX_WORKERS_PER_LINE

        else:
            df["active_workers"] = etl_config.DEFAULT_WORKERS_PER_LINE.get("default", 1)

        return df

    def shutdown(self) -> None:
        """Shut down the async I/O thread pool gracefully."""
        logger.info("Shutting down WorkerManager...")
        try:
            self._io_executor.shutdown(wait=True)
            logger.info("WorkerManager shut down successfully")
        except Exception as e:
            logger.error(f"Error during WorkerManager shutdown: {e}")
