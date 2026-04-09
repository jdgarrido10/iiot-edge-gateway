#!/usr/bin/env python3
"""
MQTT Production Data Simulator.

Publishes a clean, predictable sequence of production messages to verify
standard ETL flows, article mapping, and pipeline integrations.
"""

import time
import json
import os
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("[ERROR] Missing dependency: run 'pip install paho-mqtt'")
    import sys

    sys.exit(1)

# Configuration constants
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = 1883
TOPIC = "factory/packaging/line_01"

# Synthetic production batches
MESSAGES = [
    # Batch Alpha (Ref 1001)
    {
        "identity": 5001,
        "article_number": "1001",
        "article_name": "PROD_ALPHA_500G",
        "net_weight": 0.5025,
    },
    {
        "identity": 5002,
        "article_number": "1001",
        "article_name": "PROD_ALPHA_500G",
        "net_weight": 0.4980,
    },
    {
        "identity": 5003,
        "article_number": "1001",
        "article_name": "PROD_ALPHA_500G",
        "net_weight": 0.5051,
    },
    {
        "identity": 5004,
        "article_number": "1001",
        "article_name": "PROD_ALPHA_500G",
        "net_weight": 0.5000,
    },
    {
        "identity": 5005,
        "article_number": "1001",
        "article_name": "PROD_ALPHA_500G",
        "net_weight": 0.5012,
    },
    # Batch Beta (Ref 2001)
    {
        "identity": 6001,
        "article_number": "2001",
        "article_name": "PROD_BETA_1000G",
        "net_weight": 1.0050,
    },
    {
        "identity": 6002,
        "article_number": "2001",
        "article_name": "PROD_BETA_1000G",
        "net_weight": 0.9980,
    },
]


def on_connect(client: mqtt.Client, userdata: dict, flags: dict, rc: int) -> None:
    """Callback for when the client receives a CONNACK response from the server."""
    if rc == 0:
        print(f"[INFO] Successfully connected to MQTT broker at {BROKER_HOST}")
    else:
        print(f"[ERROR] Connection failed with result code {rc}")


def run_simulation() -> None:
    """Connect to the broker and dispatch the synthetic messages."""
    client = mqtt.Client(client_id="ProductionSimulator")
    client.on_connect = on_connect

    try:
        print(f"[INFO] Attempting connection to {BROKER_HOST}:{BROKER_PORT}...")
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        client.loop_start()

        # Brief pause to ensure connection is established
        time.sleep(1)

        print("[INFO] Commencing data injection sequence...")

        for msg in MESSAGES:
            msg["timestamp"] = (
                datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            )
            payload = json.dumps(msg)

            client.publish(TOPIC, payload)
            print(
                f"  -> Published: Ref {msg['article_number']} | Weight: {msg['net_weight']:.4f} kg"
            )

            time.sleep(1)

        print("[INFO] Simulation sequence completed.")

    except Exception as e:
        print(f"[ERROR] Simulation encountered a fatal error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    run_simulation()
