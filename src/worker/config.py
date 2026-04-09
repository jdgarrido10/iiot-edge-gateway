"""Application configuration.

All values are read from environment variables or Docker secrets files at
import time.  No side-effects (file I/O, network calls) occur until
``Config.validate()`` or ``Config.create_directories()`` is called explicitly.
"""

import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


# ==============================================================================
# Exceptions
# ==============================================================================


class ConfigValidationError(Exception):
    """Raised by ``Config.validate()`` when required settings are missing."""


# ==============================================================================
# Helpers
# ==============================================================================


def _read_secret(
    env_file_var: str, env_fallback_var: str, alt_env_var: str | None = None
) -> str | None:
    """Return a secret value, preferring a Docker secrets file over env vars.

    Resolution order:
      1. Path pointed to by *env_file_var* (Docker secrets mount).
      2. Environment variable *env_fallback_var*.
      3. Environment variable *alt_env_var* (optional alias).

    Args:
        env_file_var: Name of the env var that holds the path to the secrets file.
        env_fallback_var: Primary env var name for the plain-text value.
        alt_env_var: Optional secondary env var name (e.g. an auto-generated
            InfluxDB init variable).

    Returns:
        The secret string, or ``None`` if none of the sources are set.
    """
    secret_file = os.getenv(env_file_var)
    if secret_file and os.path.exists(secret_file):
        with open(secret_file) as fh:
            return fh.read().strip()

    value = os.getenv(env_fallback_var)
    if value:
        return value

    if alt_env_var:
        return os.getenv(alt_env_var)

    return None


# ==============================================================================
# Configuration
# ==============================================================================


class Config:
    """Centralised, read-only configuration for the IIoT worker.

    Class attributes are populated once at import time from environment
    variables.  Call ``Config.validate()`` early in the application lifecycle
    to assert that every required setting is present before connecting to any
    external service.
    """

    # --- Base directories -----------------------------------------------------
    BASE_DIR = Path("/app")
    DATA_DIR = BASE_DIR / "data"
    LOG_DIR = BASE_DIR / "logs"

    # --- MQTT -----------------------------------------------------------------
    MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
    MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
    MQTT_TOPIC = os.getenv("MQTT_TOPIC", "factory/#")
    MQTT_USER = os.getenv("MQTT_USER")
    MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
    MQTT_QOS = int(os.getenv("MQTT_QOS", 1))
    MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", 60))

    # --- MinIO ----------------------------------------------------------------
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "admin")
    MINIO_ROOT_PASSWORD = _read_secret("MINIO_PASSWORD_FILE", "MINIO_ROOT_PASSWORD")
    BUCKET_NAME = os.getenv("MINIO_BUCKET", "raw-data")
    MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

    # --- InfluxDB -------------------------------------------------------------
    INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
    INFLUX_TOKEN = _read_secret(
        "INFLUX_TOKEN_FILE",
        "INFLUX_TOKEN",
        alt_env_var="DOCKER_INFLUXDB_INIT_ADMIN_TOKEN",
    )
    INFLUX_ORG = os.getenv("DOCKER_INFLUXDB_INIT_ORG", "MyOrg")
    INFLUX_BUCKET = os.getenv("DOCKER_INFLUXDB_INIT_BUCKET", "sensors")
    INFLUX_TIMEOUT = int(os.getenv("INFLUX_TIMEOUT", 30_000))
    INFLUX_WRITE_BATCH_SIZE = int(os.getenv("INFLUX_WRITE_BATCH_SIZE", 5_000))

    # --- SMTP / email alerts --------------------------------------------------
    SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
    SMTP_USER = os.getenv("SMTP_USER")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "Production Alerts")

    # --- Batch / buffer settings ----------------------------------------------
    try:
        BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
        if not (1 <= BATCH_SIZE <= 10_000):
            raise ValueError("BATCH_SIZE must be between 1 and 10 000")
    except ValueError as exc:
        print(f"Warning: {exc}. Using default 100.", file=sys.stderr)
        BATCH_SIZE = 100

    try:
        BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", 10))
        if not (1 <= BATCH_TIMEOUT <= 300):
            raise ValueError("BATCH_TIMEOUT must be between 1 and 300 seconds")
    except ValueError as exc:
        print(f"Warning: {exc}. Using default 10.", file=sys.stderr)
        BATCH_TIMEOUT = 10

    ROLLOVER_THRESHOLD = int(os.getenv("ROLLOVER_THRESHOLD", 1_000))
    MAX_BUFFER_SIZE = int(os.getenv("MAX_BUFFER_SIZE", 10_000))
    MAX_WORKERS_PER_LINE = int(os.getenv("MAX_WORKERS_PER_LINE", 100))

    # --- Thread pools ---------------------------------------------------------
    IO_THREAD_POOL_SIZE = int(os.getenv("IO_THREAD_POOL_SIZE", 4))
    PROCESSING_THREAD_POOL_SIZE = int(os.getenv("PROCESSING_THREAD_POOL_SIZE", 2))

    # --- Circuit breaker ------------------------------------------------------
    CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", 5))
    CIRCUIT_BREAKER_TIMEOUT = int(os.getenv("CIRCUIT_BREAKER_TIMEOUT", 60))

    # --- API server -----------------------------------------------------------
    API_PORT = int(os.getenv("API_PORT", 8765))
    API_ALLOWED_ORIGINS = os.getenv(
        "ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173"
    ).split(",")

    # --- Prometheus -----------------------------------------------------------
    PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 9091))

    # --- Logging --------------------------------------------------------------
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FORMAT = os.getenv("LOG_FORMAT", "json")

    # --- External form links (optional) ---------------------------------------
    FORMS_RETURN = os.getenv("FORMS_RETURN", "")
    FORMS_STOP = os.getenv("FORMS_STOP", "")

    # --- File paths -----------------------------------------------------------
    CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"
    WORKER_CONFIG_FILE = DATA_DIR / "config_workers.json"
    TARGET_CONFIG_FILE = DATA_DIR / "config_targets.json"
    ARTICLES_MAP_FILE = DATA_DIR / "articles_map.json"
    CONTACTS_FILE = DATA_DIR / "contacts.json"
    DLQ_DIR = DATA_DIR / "dlq"
    WAL_DIR = DATA_DIR / "wal"

    # ==========================================================================
    # Class methods
    # ==========================================================================

    @classmethod
    def validate(cls) -> bool:
        """Assert that all required settings are present.

        Raises:
            ConfigValidationError: If one or more required values are missing.

        Returns:
            ``True`` when validation passes.
        """
        errors: list[str] = []

        if not cls.MINIO_ROOT_PASSWORD:
            errors.append("MINIO_ROOT_PASSWORD is not set (env var or secrets file)")
        if not cls.MINIO_ENDPOINT:
            errors.append("MINIO_ENDPOINT is not set")
        if not cls.MINIO_ROOT_USER:
            errors.append("MINIO_ROOT_USER is not set")
        if not cls.INFLUX_TOKEN:
            errors.append("INFLUX_TOKEN is not set (env var or secrets file)")
        if not cls.INFLUX_ORG:
            errors.append("INFLUX_ORG is not set")
        if not cls.INFLUX_BUCKET:
            errors.append("INFLUX_BUCKET is not set")

        if not cls.SMTP_USER or not cls.SMTP_PASSWORD:
            print(
                "Warning: SMTP credentials not fully configured — email alerts will be disabled.",
                file=sys.stderr,
            )

        if errors:
            raise ConfigValidationError(f"Configuration errors: {', '.join(errors)}")

        return True

    @classmethod
    def create_directories(cls) -> None:
        """Create all required runtime directories if they do not already exist."""
        for directory in (cls.DATA_DIR, cls.LOG_DIR, cls.DLQ_DIR, cls.WAL_DIR):
            directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def print_config(cls) -> None:
        """Print a human-readable summary of the active configuration to stdout."""
        separator = "=" * 60
        print(separator)
        print("IIoT SYSTEM CONFIGURATION")
        print(separator)
        print(f"MQTT Broker   : {cls.MQTT_BROKER}:{cls.MQTT_PORT}")
        print(f"MinIO         : {cls.MINIO_ENDPOINT}  (bucket: {cls.BUCKET_NAME})")
        print(
            f"InfluxDB      : {cls.INFLUX_URL}  (org: {cls.INFLUX_ORG}, bucket: {cls.INFLUX_BUCKET})"
        )
        print(f"SMTP user     : {cls.SMTP_USER or 'not configured'}")
        print(f"Log level     : {cls.LOG_LEVEL}")
        print(separator)
