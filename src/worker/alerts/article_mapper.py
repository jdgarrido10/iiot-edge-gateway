"""
Article code mapper with hot-reload from a JSON config file.
"""

import json
import os
import threading

import pandas as pd

from core.logger import setup_logger

logger = setup_logger(__name__)


class ArticleMapper:
    """Map raw article codes to human-readable names with automatic hot-reload.

    The config file is monitored via ``mtime`` and reloaded transparently
    whenever it changes, so mappings can be updated at runtime without
    restarting the service.

    Config file format::

        {
            "description": "Article code to name mapping",
            "default": "UNKNOWN_PRODUCT",
            "mappings": {
                "1001": "Product A",
                "1002": "Product B"
            }
        }

    Args:
        config_path: Path to the JSON mapping config file.
    """

    def __init__(self, config_path: str = "/app/data/articles_map.json"):
        self.config_path = config_path
        self._mapping: dict = {}
        self._default_value: str = "UNKNOWN"
        self._last_mtime: float = 0.0
        self._lock = threading.Lock()

        self._ensure_config_exists()
        self._load_mapping()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_config_exists(self) -> None:
        """Create a default config file if none exists at ``config_path``."""
        if os.path.exists(self.config_path):
            return

        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)

        default_config = {
            "description": "Article code to readable name mapping",
            "default": "UNKNOWN_PRODUCT",
            "mappings": {
                "1001": "Example Product A",
                "1002": "Example Product B",
            },
        }

        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(default_config, f, indent=4, ensure_ascii=False)
            logger.info(f"Default article mapping created at: {self.config_path}")
        except Exception as e:
            logger.error(f"Failed to create article mapping config: {e}")

    def _load_mapping(self) -> None:
        """Reload mapping from disk if the file has changed (thread-safe).

        No-ops if ``mtime`` has not changed since the last load.
        """
        with self._lock:
            try:
                current_mtime = os.path.getmtime(self.config_path)
                if current_mtime <= self._last_mtime:
                    return

                with open(self.config_path, encoding="utf-8") as f:
                    config = json.load(f)

                self._mapping = config.get("mappings", {})
                self._default_value = config.get("default", "UNKNOWN")
                self._last_mtime = current_mtime
                logger.info(f"Article mapping reloaded: {len(self._mapping)} entries")

            except Exception as e:
                logger.error(f"Error loading article mapping: {e}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich_dataframe(
        self, df: pd.DataFrame, target_col: str = "number_refactor"
    ) -> pd.DataFrame:
        """Add a mapped name column derived from ``article_number``.

        Args:
            df: DataFrame containing an ``article_number`` column.
            target_col: Name of the output column to create or overwrite.

        Returns:
            DataFrame with *target_col* populated from the mapping.
            Unmapped codes fall back to the configured default value.
        """
        if df.empty:
            return df

        self._load_mapping()
        df[target_col] = (
            df["article_number"]
            .astype(str)
            .str.strip()
            .map(self._mapping)
            .fillna(self._default_value)
        )
        return df

    def get_mapping(self) -> dict:
        """Return a copy of the current code-to-name mapping.

        Triggers a hot-reload check before returning.

        Returns:
            Dict mapping article code (str) → article name (str).
        """
        self._load_mapping()
        return dict(self._mapping)
