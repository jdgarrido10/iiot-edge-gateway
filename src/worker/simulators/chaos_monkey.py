#!/usr/bin/env python3
"""
MQTT Chaos Monkey utility.

Injects synthetic production data with a configurable error rate to stress-test
the buffer manager, DLQ (Dead Letter Queue), and numeric coercion logic.
"""

import time
import json
import random
import threading
import sys
import os
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("[ERROR] Missing dependency: run 'pip install paho-mqtt'")
    sys.exit(1)

# Configuration constants
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = 1883
TOPIC_BASE = "factory/packaging"


class ChaosWorker(threading.Thread):
    """Worker thread that continuously publishes semi-randomized payloads."""

    def __init__(self, line_id: str):
        super().__init__()
        self.line_id = line_id
        self.running = True
        self.client = mqtt.Client(client_id=f"ChaosWorker_{line_id}")

    def run(self) -> None:
        """Connect to the broker and start publishing messages."""
        try:
            self.client.connect(BROKER_HOST, BROKER_PORT, 60)
            while self.running:
                payload = self._generate_payload()
                topic = f"{TOPIC_BASE}/{self.line_id}/data"
                self.client.publish(topic, payload)
                time.sleep(0.05)  # Throttle to ~20 messages per second per thread
        except Exception as e:
            print(f"[ERROR] Worker {self.line_id} encountered an error: {e}")

    def _generate_payload(self) -> str:
        """Generate a payload with a probability of corruption."""
        dice = random.random()

        # 5% chance of severe JSON corruption (Triggers DLQ)
        if dice < 0.05:
            return '{"invalid_json": true, "missing_bracket": '

        # 10% chance of out-of-bounds or non-numeric weights
        if dice < 0.15:
            weight = "50,000"  # Unreasonably high weight
        else:
            weight = f"0,{random.randint(480, 520)}"

        data = {
            "Identity": int(time.time() * 1000),
            "ArticleName": "Synthetic_Product",
            "ActualNetWeightValue": weight,
            "Timestamp": datetime.now().isoformat(),
            "DeviceName": self.line_id,
            "active_workers": random.randint(1, 5),
        }
        return json.dumps(data)

    def stop(self) -> None:
        """Gracefully stop the worker thread."""
        self.running = False
        self.client.disconnect()


if __name__ == "__main__":
    print("[INFO] Starting Chaos Monkey (Press Ctrl+C to stop)")
    workers = []

    # Initialize 3 concurrent lines
    for i in range(1, 4):
        worker = ChaosWorker(f"Line_{i:02d}")
        worker.start()
        workers.append(worker)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Interrupt received. Stopping chaos workers...")
        for w in workers:
            w.stop()
        for w in workers:
            w.join()
        print("[INFO] All workers stopped safely.")
