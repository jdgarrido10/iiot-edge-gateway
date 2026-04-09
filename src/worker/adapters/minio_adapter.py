"""
MinIO storage adapter: bucket management, CSV archival, and worker change logging.
"""

import io
import json
import re
from datetime import datetime

import pandas as pd
from minio import Minio
from minio.lifecycleconfig import LifecycleConfig, Rule, Expiration
from minio.commonconfig import ENABLED, Filter
from minio.versioningconfig import VersioningConfig, ENABLED as VERSIONING_ENABLED

from config import Config
from core.logger import setup_logger

logger = setup_logger(__name__)


class MinioAdapter:
    """Adapter for reading and writing data objects in MinIO.

    On construction the adapter:
    1. Connects to the MinIO endpoint.
    2. Creates the target bucket if it does not exist.
    3. Applies a lifecycle retention policy and enables versioning
       (failures in step 3 are logged as warnings and do not abort startup).
    """

    def __init__(self):
        try:
            self.client = Minio(
                endpoint=Config.MINIO_ENDPOINT,
                access_key=Config.MINIO_ROOT_USER,
                secret_key=Config.MINIO_ROOT_PASSWORD,
                secure=Config.MINIO_SECURE,
            )
            logger.info(f"MinIO client connected: {Config.MINIO_ENDPOINT}")

        except Exception as e:
            logger.critical(f"Failed to connect to MinIO: {e}", exc_info=True)
            raise

        self._ensure_bucket()

        try:
            self._configure_lifecycle(days_to_expire=365)
            self._enable_versioning()
        except Exception as e:
            logger.warning(f"Could not apply advanced MinIO bucket policies: {e}")

    # ------------------------------------------------------------------
    # Bucket setup
    # ------------------------------------------------------------------

    def _ensure_bucket(self) -> None:
        """Create the configured bucket if it does not already exist."""
        try:
            if not self.client.bucket_exists(Config.BUCKET_NAME):
                self.client.make_bucket(Config.BUCKET_NAME)
                logger.info(f"Bucket '{Config.BUCKET_NAME}' created")
        except Exception as e:
            logger.critical(f"Error verifying/creating bucket: {e}", exc_info=True)
            raise

    def _configure_lifecycle(self, days_to_expire: int) -> None:
        """Set a lifecycle rule to expire objects under the packaging/ prefix.

        Args:
            days_to_expire: Number of days before objects expire.
        """
        try:
            config = LifecycleConfig(
                [
                    Rule(
                        ENABLED,
                        rule_filter=Filter(prefix="packaging/"),
                        rule_id="data_retention",
                        expiration=Expiration(days=days_to_expire),
                    ),
                ]
            )
            self.client.set_bucket_lifecycle(Config.BUCKET_NAME, config)
            logger.debug(
                f"Lifecycle policy set: {days_to_expire}-day expiry on packaging/"
            )
        except Exception as e:
            logger.warning(f"Error setting lifecycle policy: {e}")

    def _enable_versioning(self) -> None:
        """Enable object versioning on the configured bucket."""
        try:
            self.client.set_bucket_versioning(
                Config.BUCKET_NAME,
                VersioningConfig(VERSIONING_ENABLED),
            )
            logger.debug("Object versioning enabled")
        except Exception as e:
            logger.warning(f"Error enabling versioning: {e}")

    # ------------------------------------------------------------------
    # Public write API
    # ------------------------------------------------------------------

    def save_dataframe_as_csv(self, df: pd.DataFrame, object_name: str) -> int:
        """Serialise a DataFrame to CSV and store it in MinIO.

        Args:
            df: DataFrame to serialise. Empty DataFrames are silently skipped.
            object_name: Full object path including folder prefix and filename.

        Returns:
            Number of bytes written, or 0 on error or empty input.
        """
        if df.empty:
            return 0

        try:
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            tags = self._build_tags(df)

            self.client.put_object(
                bucket_name=Config.BUCKET_NAME,
                object_name=object_name,
                data=io.BytesIO(csv_bytes),
                length=len(csv_bytes),
                content_type="application/csv",
                metadata={
                    "row_count": str(len(df)),
                    "columns": ",".join(df.columns.tolist())[:200],
                },
                tags=tags,
            )

            filename = object_name.split("/")[-1]
            logger.info(
                f"MinIO: saved '{filename}'",
                extra={
                    "file_name": filename,
                    "quality_status": tags.get("QualityStatus", "Unknown"),
                    "size_bytes": len(csv_bytes),
                    "rows": len(df),
                },
            )
            return len(csv_bytes)

        except Exception as e:
            logger.error(f"MinIO save error: {e}", exc_info=True)
            return 0

    def save_json(self, object_name: str, data: dict) -> bool:
        """Serialise a dict to JSON and store it in MinIO.

        Args:
            object_name: Full object path including folder prefix and filename.
            data: Dictionary to serialise. Non-serialisable values are
                converted via ``str()``.

        Returns:
            True on success, False on error.
        """
        try:
            json_bytes = json.dumps(
                data, default=str, indent=2, ensure_ascii=False
            ).encode("utf-8")

            self.client.put_object(
                bucket_name=Config.BUCKET_NAME,
                object_name=object_name,
                data=io.BytesIO(json_bytes),
                length=len(json_bytes),
                content_type="application/json",
            )
            return True

        except Exception as e:
            logger.error(f"Error saving JSON to MinIO: {e}")
            return False

    def save_worker_log(self, line_id: str, count: int) -> None:
        """Archive a worker count change event to MinIO as a CSV log entry.

        Storage path::

            packaging/Manual_Config/{line_id}/workers/{year}/{month}/{day}/{time}_workers_change_{count}.csv

        Args:
            line_id: Production line identifier.
            count: New worker count value.
        """
        try:
            now = datetime.now()
            df_log = pd.DataFrame(
                [
                    {
                        "timestamp": now.isoformat(),
                        "line_id": line_id,
                        "new_worker_count": count,
                        "event": "API_UPDATE",
                    }
                ]
            )

            date_path = f"{now.year}/{now.month:02d}/{now.day:02d}"
            filename = f"{now.strftime('%H%M%S')}_workers_change_{count}.csv"
            full_path = (
                f"packaging/Manual_Config/{line_id}/workers/{date_path}/{filename}"
            )

            csv_bytes = df_log.to_csv(index=False).encode("utf-8")
            self.client.put_object(
                bucket_name=Config.BUCKET_NAME,
                object_name=full_path,
                data=io.BytesIO(csv_bytes),
                length=len(csv_bytes),
                content_type="application/csv",
            )

            logger.info(
                "Worker log saved",
                extra={"line": line_id, "workers": count, "path": full_path},
            )

        except Exception as e:
            logger.error(f"Error saving worker log to MinIO: {e}", exc_info=True)

    def get_smart_filename(self, df: pd.DataFrame) -> str:
        """Generate a descriptive filename for a data batch.

        Format: ``{HHMMSS}_{batch}_{product}_{status}.csv``

        Args:
            df: DataFrame used to extract naming metadata.

        Returns:
            Filename string safe for use as an object name component.
        """
        try:
            timestamp_str = datetime.now().strftime("%H%M%S")

            batch_str = "NoBatch"
            if "batch_code" in df.columns and not df.empty:
                batch_str = re.sub(r"[^a-zA-Z0-9_-]", "", str(df["batch_code"].iloc[0]))

            art_str = "UnknownProd"
            if "article_name" in df.columns and not df.empty:
                art_str = re.sub(r"[^a-zA-Z0-9]", "", str(df["article_name"].iloc[0]))[
                    :20
                ]

            if (
                "quality_status" in df.columns
                and (df["quality_status"] == "REJECTED").any()
            ):
                status_str = "WARNING"
            elif "quality_status" in df.columns:
                status_str = "OK"
            else:
                status_str = "RAW"

            return f"{timestamp_str}_{batch_str}_{art_str}_{status_str}.csv"

        except Exception as e:
            logger.error(f"Error generating smart filename: {e}")
            return f"backup_{datetime.now().strftime('%H%M%S')}.csv"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_tags(self, df: pd.DataFrame):
        """Build MinIO object tags from DataFrame metadata.

        Args:
            df: Source DataFrame.

        Returns:
            :class:`minio.commonconfig.Tags` instance (may be empty on error).
        """
        from minio.commonconfig import Tags

        tags = Tags(for_object=True)
        try:
            batch = (
                str(df["batch_code"].iloc[0])
                if "batch_code" in df.columns
                else "Unknown"
            )
            tags["BatchId"] = batch.replace("/", "-")[:128]

            if (
                "quality_status" in df.columns
                and (df["quality_status"] == "REJECTED").any()
            ):
                tags["QualityStatus"] = "WARNING"
            else:
                tags["QualityStatus"] = "OK"

            if "reject_flag" in df.columns and (df["reject_flag"] == 1).any():
                tags["HardwareReject"] = "TRUE"

            topic = (
                str(df["mqtt_topic"].iloc[0])
                if "mqtt_topic" in df.columns
                else "Unknown"
            )
            tags["Line"] = topic.replace("/", "_")

        except Exception:
            pass

        return tags
