"""Application entry point.

Bootstraps all services, connects to MQTT, and runs the main event loop.
Graceful shutdown is triggered by SIGTERM or SIGINT (Ctrl-C).
"""

import sys
import time
import signal
from concurrent.futures import ThreadPoolExecutor

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from config import Config, ConfigValidationError
from adapters.influx_adapter import InfluxAdapter
from adapters.minio_adapter import MinioAdapter
from adapters.mqtt_client import MQTTService
from core.api_server import APIServer
from core.buffer_manager import BufferManager
from core.logger import setup_logger
from core.pipeline import DataPipeline
from core.state_manager import StateManager
from core.target_manager import TargetManager
from core.worker_manager import WorkerManager

# ==============================================================================
# Prometheus metrics
# ==============================================================================

messages_processed = Counter(
    "iiot_messages_total",
    "Total messages processed",
    ["topic", "status"],
)
pipeline_duration = Histogram(
    "iiot_pipeline_seconds",
    "Pipeline processing time in seconds",
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0],
)
buffer_size_gauge = Gauge(
    "iiot_buffer_size",
    "Current number of messages in the buffer",
    ["topic"],
)
error_counter = Counter(
    "iiot_errors_total",
    "Total errors by type and component",
    ["error_type", "component"],
)

logger = setup_logger(__name__)


# ==============================================================================
# Application
# ==============================================================================


class Application:
    """IIoT pipeline application.

    Manages the full lifecycle of all services: MQTT ingestion, buffering,
    async pipeline execution, HTTP API, and Prometheus metrics.
    """

    def __init__(self) -> None:
        self.buffer_mgr: BufferManager = BufferManager()
        self.pipeline: DataPipeline | None = None
        self.state_mgr: StateManager | None = None
        self.worker_mgr: WorkerManager | None = None
        self.target_mgr: TargetManager | None = None
        self.mqtt_svc: MQTTService | None = None
        self.api_svc: APIServer | None = None
        self.minio_svc: MinioAdapter | None = None
        self.influx_svc: InfluxAdapter | None = None

        self._shutdown_requested: bool = False

        self._executor = ThreadPoolExecutor(
            max_workers=Config.PROCESSING_THREAD_POOL_SIZE,
            thread_name_prefix="PipelineWorker",
        )

        self._setup_signal_handlers()

    # --------------------------------------------------------------------------
    # Signal handling
    # --------------------------------------------------------------------------

    def _setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum: int, frame: object) -> None:
        logger.info("Signal %s received — initiating shutdown.", signum)
        self._shutdown_requested = True

    # --------------------------------------------------------------------------
    # MQTT callback
    # --------------------------------------------------------------------------

    def on_mqtt_message(self, client: object, userdata: object, msg: object) -> None:
        """Handle an incoming MQTT message.

        Adds the payload to the topic buffer and triggers pipeline execution
        when the buffer reaches capacity.

        Args:
            client: The MQTT client instance (unused).
            userdata: User-defined data passed by paho (unused).
            msg: The received MQTT message object.
        """
        topic: str = msg.topic
        payload: str = msg.payload.decode("utf-8")

        try:
            is_full = self.buffer_mgr.add_message(topic, payload)
            buffer_size_gauge.labels(topic=topic).set(
                self.buffer_mgr.get_buffer_size(topic)
            )
            if is_full:
                self._dispatch_topic(topic)

        except Exception:
            logger.exception("Error processing MQTT message from topic '%s'.", topic)
            error_counter.labels(error_type="mqtt_processing", component="buffer").inc()

    # --------------------------------------------------------------------------
    # Pipeline dispatch
    # --------------------------------------------------------------------------

    def _dispatch_topic(self, topic: str) -> None:
        """Submit a pipeline job for *topic* without blocking the caller.

        Args:
            topic: The MQTT topic whose buffer should be flushed.
        """
        if self._shutdown_requested:
            return

        wrapped_data = self.buffer_mgr.pop_buffer(topic)
        if not wrapped_data:
            return

        self._executor.submit(self._pipeline_worker, topic, wrapped_data)

    def _pipeline_worker(self, topic: str, wrapped_data: list) -> None:
        """Execute the ETL pipeline for one batch.

        Runs inside a thread-pool worker.  On failure the batch is restored
        to the buffer so it can be retried on the next tick.

        Args:
            topic: The MQTT topic the batch belongs to.
            wrapped_data: List of raw message payloads to process.
        """
        start_time = time.time()
        logger.debug(
            "Worker started for topic '%s' (%d items).", topic, len(wrapped_data)
        )

        try:
            with pipeline_duration.time():
                success = self.pipeline.run_pipeline(topic, wrapped_data)

            status = "success" if success else "failed"
            messages_processed.labels(topic=topic, status=status).inc(len(wrapped_data))

            if not success:
                logger.warning(
                    "Pipeline failed for topic '%s' — re-queuing %d items.",
                    topic,
                    len(wrapped_data),
                )
                self.buffer_mgr.restore_buffer(topic, wrapped_data)
                error_counter.labels(
                    error_type="pipeline_failed", component="pipeline"
                ).inc()

            elapsed = time.time() - start_time
            if elapsed > 0.5:
                logger.info("Topic '%s' processed in %.2f s.", topic, elapsed)

            buffer_size_gauge.labels(topic=topic).set(
                self.buffer_mgr.get_buffer_size(topic)
            )

        except Exception:
            logger.exception("Critical error in pipeline worker for topic '%s'.", topic)
            self.buffer_mgr.restore_buffer(topic, wrapped_data)
            error_counter.labels(error_type="thread_crash", component="pipeline").inc()

    def _flush_timed_out_buffers(self) -> None:
        """Dispatch pipeline jobs for all buffers that have exceeded their timeout."""
        for topic in self.buffer_mgr.get_ready_topics():
            self._dispatch_topic(topic)

    # --------------------------------------------------------------------------
    # Startup
    # --------------------------------------------------------------------------

    def initialize_services(self) -> None:
        """Validate configuration and start all application services.

        Exits the process with code 1 if configuration validation fails or if
        any service raises an unrecoverable error during startup.
        """
        try:
            Config.validate()
            Config.create_directories()
        except ConfigValidationError as exc:
            logger.critical("Configuration error: %s", exc)
            sys.exit(1)

        logger.info("Starting IIoT worker.")
        Config.print_config()

        try:
            # 1. Storage adapters
            self.minio_svc = MinioAdapter()
            self.influx_svc = InfluxAdapter()

            # 2. State manager
            # NOTE: WAL-based recovery was intentionally removed.  The ETL is
            # idempotent and paho-mqtt re-delivers messages on reconnect, making
            # in-flight batch recovery unnecessary.
            self.state_mgr = StateManager()

            # 3. Business-logic managers
            self.worker_mgr = WorkerManager(
                minio_adapter=self.minio_svc,
                config_path=str(Config.WORKER_CONFIG_FILE),
            )
            logger.info("WorkerManager loaded.")

            self.target_mgr = TargetManager(config_path=str(Config.TARGET_CONFIG_FILE))

            # 4. API server — mqtt_service injected after MQTT connects so the
            #    /health endpoint always reflects the real connection state.
            self.api_svc = APIServer(
                port=Config.API_PORT,
                worker_manager=self.worker_mgr,
                mqtt_service=None,
                influx_service=self.influx_svc,
            )

            # 5. ETL pipeline
            self.pipeline = DataPipeline(
                minio_svc=self.minio_svc,
                influx_svc=self.influx_svc,
                state_manager=self.state_mgr,
                worker_manager=self.worker_mgr,
                target_manager=self.target_mgr,
            )

            # 6. MQTT — connect before starting the API so the first health
            #    check already reflects the real broker connection state.
            self.mqtt_svc = MQTTService(on_message_callback=self.on_mqtt_message)
            if not self.mqtt_svc.connect_and_loop():
                raise RuntimeError("Could not connect to MQTT broker.")

            self.api_svc.context["mqtt_service"] = self.mqtt_svc

            # 7. HTTP servers
            self.api_svc.start()
            start_http_server(Config.PROMETHEUS_PORT)

            logger.info("Worker operational — waiting for data.")

        except Exception:
            logger.critical("Fatal error during initialisation.", exc_info=True)
            self.shutdown()
            sys.exit(1)

    # --------------------------------------------------------------------------
    # Main loop
    # --------------------------------------------------------------------------

    def run(self) -> None:
        """Block until a shutdown signal is received, flushing timed-out buffers each second."""
        try:
            while not self._shutdown_requested:
                time.sleep(1)
                self._flush_timed_out_buffers()
        except Exception:
            logger.critical("Unhandled fatal error in main loop.", exc_info=True)
        finally:
            self.shutdown()

    # --------------------------------------------------------------------------
    # Shutdown
    # --------------------------------------------------------------------------

    def shutdown(self) -> None:
        """Perform a graceful shutdown of all services.

        Stops external connections first, waits for in-flight pipeline jobs to
        finish, flushes remaining buffers synchronously, then tears down adapters.
        """
        logger.info("Initiating graceful shutdown.")

        if self.api_svc:
            self.api_svc.stop()
        if self.mqtt_svc:
            self.mqtt_svc.stop()

        logger.info("Waiting for in-flight pipeline jobs to complete.")
        self._executor.shutdown(wait=True)

        if self.buffer_mgr:
            for topic in self.buffer_mgr.get_safe_keys():
                self._flush_topic_sync(topic)

        if self.worker_mgr:
            self.worker_mgr.shutdown()
        if self.influx_svc:
            self.influx_svc.close()

        if self.pipeline and hasattr(self.pipeline, "notifier"):
            self.pipeline.notifier.stop()
        if self.pipeline and hasattr(self.pipeline, "prod_monitor"):
            self.pipeline.prod_monitor.stop()

        logger.info("Shutdown complete.")

    def _flush_topic_sync(self, topic: str) -> None:
        """Flush the buffer for *topic* synchronously during shutdown.

        Args:
            topic: The MQTT topic whose remaining buffer should be processed.
        """
        try:
            data = self.buffer_mgr.pop_buffer(topic)
            if data:
                logger.info(
                    "Flushing remaining buffer for topic '%s' (%d items).",
                    topic,
                    len(data),
                )
                self.pipeline.run_pipeline(topic, data)
        except Exception:
            logger.exception("Error flushing buffer for topic '%s'.", topic)


# ==============================================================================
# Entry point
# ==============================================================================

if __name__ == "__main__":
    app = Application()
    app.initialize_services()
    app.run()
