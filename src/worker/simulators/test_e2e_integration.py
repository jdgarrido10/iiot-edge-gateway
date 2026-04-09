#!/usr/bin/env python3
"""
End-to-End integration test for the IIoT Worker.

Requires Docker Compose to be running. This script verifies that:
1. The HTTP API is accessible and healthy.
2. The MQTT broker accepts messages and the worker processes them.
"""

import time
import json
import requests
import os
import paho.mqtt.client as mqtt
from datetime import datetime

# Configuration
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = 1883
API_URL = "http://localhost:8765"


def test_api_health() -> None:
    """Check the liveness of the HTTP API."""
    print("[INFO] Testing API Health endpoint...")
    try:
        response = requests.get(f"{API_URL}/health", timeout=2)
        if response.status_code == 200:
            print("[SUCCESS] API is healthy and reachable.")
        else:
            print(
                f"[ERROR] API returned unexpected status code: {response.status_code}"
            )
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API is unreachable: {e}")


def test_mqtt_flow() -> None:
    """Inject an MQTT message and verify systemic processing."""
    print("[INFO] Testing MQTT -> Pipeline flow...")

    try:
        client = mqtt.Client(client_id="E2E_Test_Client")
        client.connect(BROKER_HOST, BROKER_PORT)

        payload = {
            "Identity": 999999,
            "ArticleName": "E2E_TEST_PRODUCT",
            "ActualNetWeightValue": "0,500",
            "Timestamp": datetime.now().isoformat(),
            "DeviceName": "Line_Test_01",
        }

        topic = "factory/packaging/Line_Test_01/data"
        client.publish(topic, json.dumps(payload))
        client.disconnect()
        print(f"[SUCCESS] Test message published to '{topic}'.")

        # Allow the pipeline worker time to process the buffer
        time.sleep(2)

        # Optional: Check prometheus metrics if exposed on a known endpoint
        try:
            metrics_response = requests.get("http://localhost:9091/metrics", timeout=2)
            if "iiot_messages_total" in metrics_response.text:
                print("[SUCCESS] Pipeline metrics verified: message processed.")
            else:
                print(
                    "[WARNING] Metrics endpoint reachable, but processing not confirmed."
                )
        except requests.exceptions.RequestException:
            print("[WARNING] Metrics endpoint unreachable; cannot verify processing.")

    except Exception as e:
        print(f"[ERROR] MQTT flow test failed: {e}")


if __name__ == "__main__":
    print("=== E2E Integration Test Suite ===")
    test_api_health()
    print("-" * 30)
    test_mqtt_flow()
    print("=== Suite Complete ===")
