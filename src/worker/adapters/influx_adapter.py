"""
InfluxDB write adapter with Circuit Breaker protection and structured logging.
"""

import time
import traceback

import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from config import Config
from core.circuit_breaker import CircuitBreaker, CircuitOpenError
from core.logger import setup_logger

logger = setup_logger(__name__)

# Columns that must be written as numeric fields
_NUMERIC_FIELDS = [
    "active_workers",
    "net_weight",
    "giveaway_g",
    "identity",
    "target_value",
    "clean_net_weight_g",
    "tare_g",
    "target_weight_g",
    "quality_mismatch",
    "reject_flag",
    "batch_sequence",
]

# Subset of _NUMERIC_FIELDS that must be written as integers
_INTEGER_FIELDS = {
    "batch_sequence",
    "identity",
    "active_workers",
    "reject_flag",
    "quality_mismatch",
}

# Columns written as InfluxDB tag strings (low-cardinality metadata)
_TAG_COLUMNS = [
    "linea",
    "batch_instance_id",
    "article_name",
    "quality_status",
    "shift",
    "batch_code",
    "mqtt_topic",
    "article_number",
    "machine_name",
    "machine_id",
    "operator_id",
    "number_refactor",
]


class InfluxAdapter:
    """Write pandas DataFrames to InfluxDB, protected by a Circuit Breaker.

    Connection errors or repeated write failures open the circuit and cause
    subsequent calls to return 0 immediately (fast-fail), preventing thread
    pile-ups while the database is unavailable.
    """

    def __init__(self):
        try:
            self.client = InfluxDBClient(
                url=Config.INFLUX_URL,
                token=Config.INFLUX_TOKEN,
                org=Config.INFLUX_ORG,
                timeout=Config.INFLUX_TIMEOUT,
            )
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.circuit_breaker = CircuitBreaker(
                failure_threshold=Config.CIRCUIT_BREAKER_THRESHOLD,
                timeout=Config.CIRCUIT_BREAKER_TIMEOUT,
            )
            logger.info("InfluxDB connection established")

        except Exception as e:
            logger.critical(f"Fatal error connecting to InfluxDB: {e}", exc_info=True)
            raise

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_dataframe(self, df: pd.DataFrame) -> int:
        """Write a DataFrame to InfluxDB, protected by the circuit breaker.

        Args:
            df: DataFrame to write. Must contain ``identity`` and ``timestamp``
                columns.

        Returns:
            Number of records written, or 0 on error or open circuit.
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame received — skipping write")
            return 0

        try:
            return self.circuit_breaker.call(self._write_dataframe_impl, df)

        except CircuitOpenError as e:
            logger.warning(f"Circuit open: {e} — data preserved in MinIO only")
            return 0

        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {e}")
            logger.debug(traceback.format_exc())
            return 0

    def close(self) -> None:
        """Flush pending writes and close the InfluxDB connection."""
        try:
            logger.info("Flushing InfluxDB write buffer...")
            self.write_api.close()
            self.client.close()
            logger.info("InfluxDB adapter disconnected")
        except Exception as e:
            logger.error(f"Error closing InfluxDB adapter: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Internal write implementation
    # ------------------------------------------------------------------

    def _write_dataframe_impl(self, df: pd.DataFrame) -> int:
        """Prepare and write the DataFrame. Called inside the circuit breaker.

        Steps:
        1. Validate and sort by ``identity``.
        2. Set the DatetimeIndex from ``timestamp``.
        3. Coerce numeric and string fields.
        4. Write to InfluxDB and log a summary.

        Args:
            df: Raw DataFrame from the ETL pipeline.

        Returns:
            Number of records written.

        Raises:
            Exception: Any write error is re-raised so the circuit breaker
                can record the failure.
        """
        start_time = time.time()
        df = df.copy()

        # --- Validate identity column ---
        if "identity" not in df.columns:
            logger.error("DataFrame rejected: missing 'identity' column")
            return 0

        df["identity"] = pd.to_numeric(df["identity"], errors="coerce")
        df.dropna(subset=["identity"], inplace=True)
        df.sort_values(by="identity", ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)

        if df.empty:
            logger.warning("DataFrame empty after identity validation")
            return 0

        min_id = df["identity"].min()
        max_id = df["identity"].max()

        # --- Build DatetimeIndex from timestamp ---
        if "timestamp" in df.columns:
            try:
                base_times = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
                invalid_count = base_times.isna().sum()
                if invalid_count > 0:
                    logger.warning(
                        f"{invalid_count} invalid timestamps — filling with current UTC"
                    )
                    base_times = base_times.fillna(pd.Timestamp.now(tz="UTC"))

                df.index = base_times
                df.drop(columns=["timestamp"], inplace=True)

            except Exception as e:
                logger.error(f"Error processing timestamps: {e}")
                df.index = pd.Timestamp.now(tz="UTC")
        else:
            logger.error(
                "DataFrame has no timestamp column — generating synthetic index"
            )
            now = pd.Timestamp.now(tz="UTC")
            offset_us = (df.index % 1000) * 1000
            df.index = now + pd.to_timedelta(offset_us, unit="us")

        if not df.index.is_unique:
            dup_count = df.index.duplicated().sum()
            logger.warning(
                f"{dup_count} duplicate timestamp(s) detected — InfluxDB will overwrite "
                "records sharing the same timestamp+tags. Verify ETL jitter configuration."
            )

        # --- Numeric fields ---
        for col in _NUMERIC_FIELDS:
            if col not in df.columns:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce")
            if col != "identity":
                df[col] = df[col].fillna(0)
            if col in _INTEGER_FIELDS:
                try:
                    df[col] = df[col].astype(int)
                except Exception:
                    logger.warning(f"Could not cast '{col}' to int — writing as float")

        # --- Datetime fields serialised to strings ---
        if "batch_start_time" in df.columns:
            col = df["batch_start_time"]
            if pd.api.types.is_datetime64_any_dtype(col):
                try:
                    df["batch_start_time"] = col.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                except Exception:
                    pass
            df["batch_start_time"] = (
                df["batch_start_time"]
                .fillna("unknown")
                .astype(str)
                .str.replace("nan", "unknown", case=False)
                .str.strip()
            )

        # --- Tag columns ---
        present_tags = [c for c in _TAG_COLUMNS if c in df.columns]
        if present_tags:
            df[present_tags] = df[present_tags].fillna("unknown").astype(str)
            for col in ("quality_status", "shift"):
                if col in df.columns:
                    df[col] = df[col].str.strip().str.upper()

        # --- Write ---
        self.write_api.write(
            bucket=Config.INFLUX_BUCKET,
            org=Config.INFLUX_ORG,
            record=df,
            data_frame_measurement_name="packaging_line",
            data_frame_tag_columns=present_tags,
        )

        # --- Summary log ---
        elapsed = time.time() - start_time
        rejects = (
            (df["quality_status"] == "REJECTED").sum()
            if "quality_status" in df.columns
            else 0
        )
        line_id = (
            ",".join(map(str, df["linea"].unique())) if "linea" in df.columns else "N/A"
        )
        if "batch_instance_id" in df.columns:
            unique_batches = df["batch_instance_id"].unique().tolist()
            batch_id = (
                f"{len(unique_batches)} batches"
                if len(unique_batches) > 3
                else ",".join(map(str, unique_batches))
            )
        else:
            batch_id = "N/A"

        logger.info(
            f"InfluxDB write: {len(df)} records | "
            f"lines=[{line_id}] | batches=[{batch_id}] | "
            f"rejects={rejects} | ids={min_id}-{max_id} | {elapsed:.3f}s"
        )

        return len(df)
