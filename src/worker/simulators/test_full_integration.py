"""
Full integration test suite for the ProductionMonitor and AlertNotifier.

This test simulates a real-world scenario involving multiple batches,
detecting a global return, and verifying that the alert email is queued
correctly without requiring a live MinIO instance or a real SMTP server
during the logical validation phase.
"""

import sys
import time
import shutil
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure the app root is in the path for direct execution
sys.path.append("/app")

from alerts.production_monitor import ProductionMonitor
from alerts.production_alert import AlertNotifier
from core.logger import setup_logger

logger = setup_logger("IntegrationTest")


class MockMinio:
    """Mock MinIO adapter to bypass network calls during tests."""

    def __init__(self):
        self.client = self

    def put_object(self, *args, **kwargs) -> None:
        """Mock the put_object method to do nothing."""
        pass


class MockMapper:
    """Mock ArticleMapper to bypass JSON config loading during tests."""

    def enrich_dataframe(
        self, df: pd.DataFrame, target_col: str = "number_refactor"
    ) -> pd.DataFrame:
        """Simulate an identity mapping (article_number -> target_col)."""
        if "article_number" in df.columns:
            df[target_col] = df["article_number"]
        return df


def run_full_integration_test() -> None:
    """Execute the end-to-end logical and notification test."""
    logger.info("Starting full integration test: Logic + Email Queue")

    test_state_dir = Path("/app/data/test_monitor_state")
    if test_state_dir.exists():
        shutil.rmtree(test_state_dir)

    monitor = ProductionMonitor(minio_adapter=MockMinio(), mapper=MockMapper())
    monitor.LOCAL_STATE_DIR = test_state_dir

    # AlertNotifier will use the real contacts.json and .env credentials
    notifier = AlertNotifier()
    topic = "factory/packaging/zone_A"

    try:
        now = datetime.now(timezone.utc)

        # Step 1: Produce Article Alpha (2 hours ago)
        logger.info("Step 1: Producing Article Alpha (Historical data)...")
        df1 = pd.DataFrame(
            [
                {
                    "timestamp": now - timedelta(hours=2),
                    "article_number": "PROD_ALPHA_500G",
                    "DeviceName": "Line_01",
                }
            ]
        )
        monitor.process_chunk(df1, topic)

        # Step 2: Switch to Article Beta (1 hour ago) -> Closes Alpha batch
        logger.info("Step 2: Switching to Article Beta (Closes previous batch)...")
        df2 = pd.DataFrame(
            [
                {
                    "timestamp": now - timedelta(hours=1),
                    "article_number": "PROD_BETA_1000G",
                    "DeviceName": "Line_01",
                }
            ]
        )
        monitor.process_chunk(df2, topic)

        # Step 3: Return to Article Alpha -> Should trigger a GLOBAL RETURN alert
        logger.info("Step 3: Returning to Article Alpha (Triggering alert)...")
        df3 = pd.DataFrame(
            [
                {
                    "timestamp": now,
                    "article_number": "PROD_ALPHA_500G",
                    "DeviceName": "Line_02",
                }
            ]
        )

        _, alerts = monitor.process_chunk(df3, topic)

        # Step 4: Verification and Dispatch
        if not alerts:
            logger.error("Logical Failure: Monitor did not generate any alerts.")
            return

        logger.info(f"Logical Success: Detected {len(alerts)} alert(s).")

        for alert in alerts:
            logger.info(f"Processing alert: {alert['msg']}")
            notifier.send_alert(
                topic=alert["topic"], message=alert["msg"], alert_type=alert["type"]
            )
            logger.info("Alert queued in the mail spooler.")

        # Step 5: Wait for the worker thread to process the queue
        logger.info("Waiting for the email worker thread to drain the queue...")
        timeout = 10
        start_wait = time.time()

        while notifier._mailbox.qsize() > 0:
            if time.time() - start_wait > timeout:
                logger.error("Timeout: The email queue was not processed.")
                break
            time.sleep(1)

        if notifier._mailbox.qsize() == 0:
            logger.info(
                "Queue Empty: The system successfully attempted to send the email."
            )

    except Exception as e:
        logger.error(f"Test failed due to an exception: {e}", exc_info=True)
    finally:
        notifier.stop()
        if test_state_dir.exists():
            shutil.rmtree(test_state_dir)
        logger.info("Integration test complete.")


if __name__ == "__main__":
    run_full_integration_test()
