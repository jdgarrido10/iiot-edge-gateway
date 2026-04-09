"""
Industrial production data pipeline: ETL → deduplication → enrichment → storage.
"""

import pandas as pd
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from config import Config
from .etl import process_node_red_payload
from .state_manager import StateManager
from .worker_manager import WorkerManager
from .target_manager import TargetManager
from adapters.minio_adapter import MinioAdapter
from adapters.influx_adapter import InfluxAdapter
from core.logger import setup_logger

from alerts.production_monitor import ProductionMonitor
from alerts.article_mapper import ArticleMapper
from alerts.production_alert import AlertNotifier

logger = setup_logger(__name__)


class DataPipeline:
    """End-to-end processing pipeline for industrial production data.

    Processing stages:
    1. **ETL** — JSON normalisation, timestamp parsing, weight cleaning, enrichment.
    2. **Deduplication** — identity-based filtering via :class:`StateManager`.
    3. **Enrichment** — worker counts (:class:`WorkerManager`) and weight targets
       (:class:`TargetManager`).
    4. **Production monitoring** — article-change detection, ``batch_instance_id``
       assignment, return/rework detection, and email alerts via
       :class:`ProductionMonitor`.
    5. **Atomic write** — MinIO (CSV archive) then InfluxDB (time-series metrics).
    6. **Checkpoint** — persist last-processed identity via :class:`StateManager`.

    Args:
        minio_svc: MinIO adapter for CSV archival.
        influx_svc: InfluxDB adapter for metric writes.
        state_manager: Cross-restart deduplication and checkpointing.
        worker_manager: Per-line active worker count enrichment.
        target_manager: Per-product weight target enrichment.
    """

    def __init__(
        self,
        minio_svc: MinioAdapter,
        influx_svc: InfluxAdapter,
        state_manager: StateManager,
        worker_manager: WorkerManager,
        target_manager: TargetManager,
    ):
        self.minio = minio_svc
        self.influx = influx_svc
        self.state_manager = state_manager
        self.worker_manager = worker_manager
        self.target_manager = target_manager

        try:
            self.MADRID_TZ = ZoneInfo("Europe/Madrid")
        except Exception:
            self.MADRID_TZ = timezone.utc

        self.mapper = ArticleMapper(str(Config.ARTICLES_MAP_FILE))
        self.notifier = AlertNotifier(str(Config.CONTACTS_FILE))
        self.prod_monitor = ProductionMonitor(
            minio_adapter=minio_svc,
            mapper=self.mapper,
            alert_notifier=self.notifier,
        )

        logger.info("DataPipeline initialised")

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run_pipeline(self, topic: str, wrapped_buffer: list) -> bool:
        """Process a complete data batch from a single MQTT topic.

        Args:
            topic: MQTT topic the batch originated from.
            wrapped_buffer: List of wrapped message dicts from :class:`BufferManager`.

        Returns:
            True on success, False if a fatal error prevents data from being saved.
        """
        raw_data = [item["payload"] for item in wrapped_buffer]
        logger.info(f"Processing batch of {len(raw_data)} records from: {topic}")

        try:
            start_time = datetime.now(timezone.utc)

            # Stage 1: ETL
            df_influx, df_minio = process_node_red_payload(raw_data)

            if df_influx is None or df_influx.empty:
                return True

            df_influx = self._normalize_timestamps(df_influx)
            if df_minio is not None:
                df_minio = self._normalize_timestamps(df_minio)

            # Stage 2: Deduplication
            df_influx = self._filter_duplicates(df_influx, topic)

            if df_influx.empty:
                logger.info(f"All records from '{topic}' were duplicates — skipping")
                return True

            if (
                df_minio is not None
                and "record_uuid" in df_influx.columns
                and "record_uuid" in df_minio.columns
            ):
                valid_uuids = df_influx["record_uuid"].tolist()
                df_minio = df_minio[df_minio["record_uuid"].isin(valid_uuids)]

            # Stage 3: Enrichment
            try:
                df_influx = self.worker_manager.enrich_dataframe(df_influx)
                if df_minio is not None and not df_minio.empty:
                    df_minio = self.worker_manager.enrich_dataframe(df_minio)
            except Exception as e:
                logger.warning(f"WorkerManager enrichment error: {e}")

            try:
                df_influx = self.target_manager.enrich_dataframe(df_influx)
                if df_minio is not None and not df_minio.empty:
                    df_minio = self.target_manager.enrich_dataframe(df_minio)
            except Exception as e:
                logger.warning(f"TargetManager enrichment error: {e}")

            # Stage 4: Production monitoring
            try:
                df_influx, alerts = self.prod_monitor.process_chunk(df_influx, topic)

                if "batch_instance_id" in df_influx.columns:
                    logger.info(
                        f"Batch tracking: {df_influx['batch_instance_id'].nunique()} batch(es) detected"
                    )

                for alert in alerts or []:
                    logger.warning(alert["msg"])

                if df_minio is not None and not df_minio.empty:
                    df_minio = self._propagate_batch_metadata(df_influx, df_minio)

            except Exception as e:
                # Non-fatal: monitoring failure must not prevent data from being saved
                logger.error(f"ProductionMonitor error: {e}", exc_info=True)

            # Stage 5: Write — MinIO then InfluxDB
            if df_minio is not None and not df_minio.empty:
                group_col = next(
                    (
                        c
                        for c in ("linea", "DeviceName", "device_name")
                        if c in df_minio.columns
                    ),
                    None,
                )
                if group_col:
                    minio_ok = all(
                        self._save_to_minio(topic, line_df)
                        for _, line_df in df_minio.groupby(group_col)
                    )
                else:
                    minio_ok = self._save_to_minio(topic, df_minio)

                if not minio_ok:
                    logger.error("Aborting pipeline: MinIO write failed")
                    return False

            if not self._save_to_influx(topic, df_influx):
                logger.error("Aborting pipeline: InfluxDB write failed")
                return False

            # Stage 6: Checkpoint
            self._save_checkpoint(df_influx, topic)

            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            logger.info(
                f"Pipeline completed for '{topic}' — {len(df_influx)} records in {elapsed:.2f}s"
            )
            return True

        except Exception as e:
            logger.critical(
                f"Critical pipeline error for '{topic}': {e}", exc_info=True
            )
            return False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _normalize_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure the ``timestamp`` column is a UTC-aware datetime Series."""
        if df is None or df.empty or "timestamp" not in df.columns:
            return df

        try:
            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            elif df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
            else:
                df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
        except Exception as e:
            logger.warning(f"Timestamp normalisation error: {e}")

        return df

    def _filter_duplicates(self, df: pd.DataFrame, topic: str) -> pd.DataFrame:
        """Remove records already processed according to :class:`StateManager`.

        Uses row-wise ``apply()`` because :meth:`StateManager.is_already_processed`
        maintains per-topic lock state and cannot be trivially vectorised.
        For typical batch sizes this overhead is acceptable. If batches grow
        significantly, consider pre-fetching the last identity per topic and
        doing a vectorised comparison instead.

        Args:
            df: DataFrame to filter.
            topic: Default MQTT topic used when ``mqtt_topic`` column is absent.

        Returns:
            DataFrame with duplicate rows removed.
        """
        try:

            def _is_duplicate(row) -> bool:
                identity = (
                    row.get("identity")
                    if "identity" in row.index
                    else row.get("Identity", 0)
                )
                row_topic = row["mqtt_topic"] if "mqtt_topic" in row.index else topic
                return self.state_manager.is_already_processed(identity, row_topic)

            mask = df.apply(_is_duplicate, axis=1)

            if mask.any():
                logger.info(f"Duplicates removed: {mask.sum()}")
                return df[~mask]

            return df

        except Exception as e:
            logger.warning(f"Duplicate filtering error: {e}")
            return df

    def _propagate_batch_metadata(
        self, df_influx: pd.DataFrame, df_minio: pd.DataFrame
    ) -> pd.DataFrame:
        """Copy batch tracking columns from the processed DataFrame to MinIO output.

        When ``record_uuid`` is available in both DataFrames, columns
        ``batch_instance_id``, ``batch_sequence``, and ``batch_start_time``
        are joined by UUID. Without UUIDs there is no safe way to align rows
        after duplicate filtering, so batch metadata is intentionally omitted
        to prevent silent data corruption.

        Also maps ``number_refactor`` from the article mapper when present.

        Args:
            df_influx: Processed DataFrame with batch metadata columns.
            df_minio: MinIO DataFrame to enrich.

        Returns:
            Enriched ``df_minio``.
        """
        batch_cols = ["batch_instance_id", "batch_sequence", "batch_start_time"]
        available_cols = [c for c in batch_cols if c in df_influx.columns]

        if available_cols:
            if "record_uuid" in df_influx.columns and "record_uuid" in df_minio.columns:
                df_minio = df_minio.merge(
                    df_influx[["record_uuid"] + available_cols],
                    on="record_uuid",
                    how="left",
                )
            else:
                logger.warning(
                    "No record_uuid available — batch metadata not propagated to MinIO. "
                    "Fields batch_instance_id/batch_sequence will be absent."
                )

        if "number_refactor" in df_influx.columns:
            mapping_dict = self.prod_monitor.mapper.get_mapping()
            source_col = next(
                (
                    c
                    for c in ("article_number", "ArticleNumber", "number")
                    if c in df_minio.columns
                ),
                None,
            )
            if source_col:
                df_minio["number_refactor"] = (
                    df_minio[source_col]
                    .astype(str)
                    .str.strip()
                    .map(mapping_dict)
                    .fillna("UNKNOWN_REF")
                )

        return df_minio

    def _save_to_minio(self, topic: str, df: pd.DataFrame) -> bool:
        """Persist a DataFrame to MinIO as a CSV file.

        The storage path follows the hierarchy::

            packaging/{topic}/{line}/{year}/{month}/{day}/{article}/{batch_id}/{filename}

        Args:
            topic: MQTT topic (used as top-level folder, slashes replaced by underscores).
            df: DataFrame to save (may be a per-line subset).

        Returns:
            True on success, False on error or if no bytes were written.
        """
        if df is None or df.empty:
            return True

        try:
            raw_topic = (
                str(df["mqtt_topic"].iloc[0]) if "mqtt_topic" in df.columns else topic
            )
            safe_topic = raw_topic.replace("/", "_")

            # Resolve line folder
            line_folder = "Unknown_Line"
            for col in ("linea", "DeviceName", "device_name"):
                if col in df.columns:
                    line_folder = str(df[col].iloc[0]).strip()
                    break
            else:
                if "mqtt_topic" in df.columns:
                    try:
                        line_folder = str(df["mqtt_topic"].iloc[0]).split("/")[-1]
                    except Exception:
                        pass

            # Resolve date path in factory timezone
            if "timestamp" in df.columns:
                first_ts = pd.to_datetime(df["timestamp"].iloc[0])
                if first_ts.tz is None:
                    first_ts = first_ts.tz_localize("UTC")
                first_ts = first_ts.astimezone(self.MADRID_TZ)
            else:
                first_ts = datetime.now(self.MADRID_TZ)

            date_path = f"{first_ts.year}/{first_ts.month:02d}/{first_ts.day:02d}"

            # Resolve article folder (sanitised)
            article_folder = "Unknown_article"
            for col in ("number_refactor", "article_name"):
                if col in df.columns:
                    article_folder = str(df[col].iloc[0]).strip()
                    break
            article_folder = "".join(
                c for c in article_folder if c.isalnum() or c in " -_"
            )

            folder = (
                f"packaging/{safe_topic}/{line_folder}/{date_path}/{article_folder}"
            )

            if "batch_instance_id" in df.columns:
                batch_id = str(df["batch_instance_id"].iloc[0])
                safe_batch = (
                    "".join(c for c in batch_id if c.isalnum() or c in "._-")
                    or "UNKNOWN_BATCH"
                )
                folder = f"{folder}/{safe_batch}"

            filename = self.minio.get_smart_filename(df)
            full_path = f"{folder}/{filename}"

            bytes_saved = self.minio.save_dataframe_as_csv(df, full_path)

            if bytes_saved > 0:
                logger.info(f"MinIO: saved '{full_path}' ({bytes_saved} bytes)")
                return True

            return False

        except Exception as e:
            logger.error(f"MinIO save error: {e}", exc_info=True)
            return False

    def _save_to_influx(self, topic: str, df: pd.DataFrame) -> bool:
        """Write a DataFrame of metrics to InfluxDB.

        Args:
            topic: MQTT topic (used for logging context only).
            df: DataFrame to write.

        Returns:
            True on success, False on error.
        """
        try:
            count = self.influx.write_dataframe(df)
            logger.info(f"InfluxDB: {count} metric(s) written ({topic})")
            return True
        except Exception as e:
            logger.error(f"InfluxDB write error: {e}", exc_info=True)
            return False

    def _save_checkpoint(self, df: pd.DataFrame, topic: str) -> None:
        """Persist the processing progress checkpoint via :class:`StateManager`.

        When the DataFrame contains a ``mqtt_topic`` column with multiple
        distinct topics, each sub-topic is checkpointed independently.

        Args:
            df: Processed DataFrame.
            topic: Default topic used when ``mqtt_topic`` column is absent.
        """
        try:
            if "mqtt_topic" in df.columns:
                for sub_topic in df["mqtt_topic"].unique():
                    self.state_manager.save_checkpoint(
                        df[df["mqtt_topic"] == sub_topic], sub_topic
                    )
            else:
                self.state_manager.save_checkpoint(df, topic)
        except Exception as e:
            logger.warning(f"Checkpoint save error: {e}")

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def get_production_status(self, topic: str) -> dict:
        """Return the current production state for a topic from :class:`ProductionMonitor`.

        Intended for monitoring APIs and real-time dashboards.

        Args:
            topic: MQTT topic to query.

        Returns:
            Dict with production monitor state and current timestamps,
            or an error dict on failure.
        """
        try:
            state_data = self.prod_monitor.state_memory.get(topic, {})
            madrid_now = datetime.now(self.MADRID_TZ)
            return {
                "topic": topic,
                "production_monitor": state_data,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "timestamp_local": madrid_now.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "active",
            }
        except Exception as e:
            logger.error(f"Error fetching production status for '{topic}': {e}")
            return {"error": str(e)}
