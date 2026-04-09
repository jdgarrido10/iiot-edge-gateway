#!/usr/bin/env python3
"""
Configuration Validator Utility.

Performs a pre-flight check to ensure all required environment variables
are present before starting the IIoT Worker. Designed to be executed as
a standalone script in CI/CD pipelines or deployment hooks.
"""

import os
import sys
from pathlib import Path

# Terminal color codes for visual feedback
COLOR_GREEN = "\033[92m"
COLOR_RED = "\033[91m"
COLOR_YELLOW = "\033[93m"
COLOR_RESET = "\033[0m"

# Core variables required for the worker to boot successfully
REQUIRED_VARS = [
    "MQTT_BROKER",
    "MQTT_PORT",
    "MQTT_TOPIC",
    "MINIO_ENDPOINT",
    "MINIO_BUCKET",
    "INFLUX_URL",
]


def load_environment() -> bool:
    """Attempt to load environment variables from local or infra .env files.

    Returns:
        True if a .env file was successfully loaded, False otherwise.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        print(
            f"{COLOR_YELLOW}[WARNING] 'python-dotenv' not installed. Relying strictly on system environment variables.{COLOR_RESET}"
        )
        return False

    # 1. Attempt local directory
    if Path(".env").exists():
        load_dotenv(".env")
        return True

    # 2. Attempt infrastructure directory (assuming standard repo layout)
    infra_env = Path(__file__).resolve().parent.parent.parent / "infra" / ".env"
    if infra_env.exists():
        print(f"[INFO] Loading environment from: {infra_env}")
        load_dotenv(infra_env)
        return True

    return False


def run_validation() -> None:
    """Execute the configuration check and exit with appropriate status codes."""
    print(f"\n{COLOR_GREEN}=== SYSTEM CONFIGURATION VALIDATION ==={COLOR_RESET}")

    if not load_environment():
        print(
            f"{COLOR_YELLOW}[WARNING] No .env file located. Using system environment variables exclusively.{COLOR_RESET}"
        )

    missing_vars = []

    for var in REQUIRED_VARS:
        val = os.getenv(var)
        if val:
            print(f" [OK]   {var:20} = {val}")
        else:
            print(f"{COLOR_RED} [FAIL] {var:20} IS MISSING{COLOR_RESET}")
            missing_vars.append(var)

    # Special check for secrets (which might be passed via Docker Secrets instead of raw env vars)
    if not os.getenv("MINIO_ROOT_PASSWORD") and not os.getenv(
        "MINIO_ROOT_PASSWORD_FILE"
    ):
        print(
            f"{COLOR_YELLOW} [WARN] MINIO_ROOT_PASSWORD not defined (Safe if using Docker Secrets).{COLOR_RESET}"
        )

    print("-" * 50)

    if missing_vars:
        print(
            f"{COLOR_RED}[FATAL] Validation failed. Missing {len(missing_vars)} required variable(s).{COLOR_RESET}\n"
        )
        sys.exit(1)
    else:
        print(
            f"{COLOR_GREEN}[SUCCESS] Configuration valid. System is ready to start.{COLOR_RESET}\n"
        )
        sys.exit(0)


if __name__ == "__main__":
    run_validation()
