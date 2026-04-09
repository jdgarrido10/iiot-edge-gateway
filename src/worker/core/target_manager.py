"""
Per-product weight target manager with hot-reload from a JSON config file.
"""

import json
import os
import threading

import pandas as pd

from core.logger import setup_logger

logger = setup_logger(__name__)


class TargetManager:
    """Load and cache per-product weight targets from a JSON config file.

    The config file is monitored via ``mtime`` and reloaded automatically
    when it changes, so targets can be updated at runtime without restarting
    the service.

    Config file format::

        {
            "12345": 500.0,
            "Salmon Ahumado": 250.0,
            "default": 20.0
        }

    Args:
        config_path: Path to the JSON targets config file.
    """

    def __init__(self, config_path: str = "/app/data/config_targets.json"):
        self.config_path = config_path
        self._lock = threading.Lock()
        self._cache: dict = {}
        self._mtime: float = 0.0

        self._ensure_config_exists()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_config_exists(self) -> None:
        """Create a minimal default config file if none exists."""
        if os.path.exists(self.config_path):
            return

        default_config = {"default": 20.0}
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, "w") as f:
                json.dump(default_config, f, indent=2)
            logger.info(f"TargetManager: default config created at {self.config_path}")
        except Exception as e:
            logger.warning(f"TargetManager: could not create default config: {e}")

    def _load_targets(self) -> dict:
        """Return cached targets, reloading from disk if the file has changed.

        Thread-safe: ``_cache`` and ``_mtime`` are always accessed under ``_lock``.

        Returns:
            Dict mapping product key → target value (float).
        """
        with self._lock:
            try:
                current_mtime = os.path.getmtime(self.config_path)
            except OSError as e:
                logger.warning(f"TargetManager: cannot stat config file: {e}")
                return self._cache

            if current_mtime > self._mtime:
                try:
                    with open(self.config_path) as f:
                        self._cache = json.load(f)
                    self._mtime = current_mtime
                    logger.debug("TargetManager: config reloaded from disk")
                except Exception as e:
                    logger.error(f"TargetManager: error loading config: {e}")

            return self._cache

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_target_for_product(self, product_key: str) -> float:
        """Return the target weight for a single product key.

        Falls back to the ``"default"`` entry, then to ``0.0``.

        Args:
            product_key: Article number or name to look up.

        Returns:
            Target weight as a float.
        """
        targets = self._load_targets()
        key = str(product_key).strip()
        return targets.get(key, targets.get("default", 0.0))

    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add a ``target_value`` column resolved from article identifiers.

        Lookup priority:
        1. ``article_number`` (numeric code, preferred — no typos).
        2. ``article_name``   (human-readable name, fallback).
        3. ``"default"`` key  (final fallback).

        Args:
            df: DataFrame containing product identifier columns.

        Returns:
            DataFrame with an added ``target_value`` float column.
        """
        if df.empty:
            return df

        raw_targets = self._load_targets()
        targets_map = {str(k).strip(): v for k, v in raw_targets.items()}
        default_val = float(raw_targets.get("default", 0.0))

        df["target_value"] = float("nan")

        if "article_number" in df.columns:
            df["target_value"] = (
                df["article_number"].astype(str).str.strip().map(targets_map)
            )

        if "article_name" in df.columns:
            df["target_value"] = df["target_value"].fillna(
                df["article_name"].astype(str).str.strip().map(targets_map)
            )

        df["target_value"] = pd.to_numeric(
            df["target_value"].fillna(default_val), errors="coerce"
        ).fillna(0.0)

        return df
