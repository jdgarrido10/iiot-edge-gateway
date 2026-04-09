"""
Test suite for BufferManager: concurrency, batch triggering, and DLQ handling.
"""

import pytest
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from core.buffer_manager import BufferManager
from config import Config

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def mock_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate DLQ storage and lower thresholds for rapid testing.

    Args:
        tmp_path: Pytest temporary directory fixture.
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        Path to the isolated Dead Letter Queue directory.
    """
    dlq_dir = tmp_path / "dlq"
    dlq_dir.mkdir()

    monkeypatch.setattr(Config, "DLQ_DIR", dlq_dir)
    monkeypatch.setattr(Config, "MAX_BUFFER_SIZE", 5)
    monkeypatch.setattr(Config, "BATCH_SIZE", 5)

    return dlq_dir


@pytest.fixture
def buffer_mgr(mock_config: Path) -> BufferManager:
    """Return a clean instance of BufferManager."""
    return BufferManager()


# =====================================================================
# Core Logic & Threshold Tests
# =====================================================================


def test_add_message_under_threshold(buffer_mgr: BufferManager):
    """Verify that adding messages below BATCH_SIZE does not trigger a flush."""
    is_ready = buffer_mgr.add_message("factory/packaging/line_1", '{"weight": 500}')
    assert is_ready is False
    assert buffer_mgr.get_buffer_size("factory/packaging/line_1") == 1


def test_add_message_triggers_batch(buffer_mgr: BufferManager):
    """Verify that reaching BATCH_SIZE triggers a ready state."""
    buffer_mgr._max_items = 5
    topic = "factory/packaging/line_2"

    for i in range(4):
        buffer_mgr.add_message(topic, f'{{"id": {i}}}')

    is_ready = buffer_mgr.add_message(topic, '{"id": 5}')

    assert is_ready is True
    assert buffer_mgr.get_buffer_size(topic) == 5


# =====================================================================
# Resilience & DLQ Tests
# =====================================================================


def test_invalid_json_goes_to_dlq(buffer_mgr: BufferManager, mock_config: Path):
    """Ensure malformed JSON payloads are gracefully routed to the DLQ."""
    is_ready = buffer_mgr.add_message("factory/error_topic", "{malformed_json:")

    assert is_ready is False
    assert buffer_mgr.get_buffer_size("factory/error_topic") == 0

    dlq_files = list(mock_config.glob("*.json"))
    assert len(dlq_files) == 1
    with open(dlq_files[0], "r") as f:
        data = json.load(f)
        assert data["reason"] == "json_decode_error"


def test_buffer_overflow_goes_to_dlq(buffer_mgr: BufferManager, mock_config: Path):
    """Ensure messages exceeding MAX_BUFFER_SIZE are discarded to prevent OOM errors."""
    topic = "factory/overflow_topic"

    for i in range(5):
        buffer_mgr.add_message(topic, '{"val": 1}')

    buffer_mgr.add_message(topic, '{"val": "overflow"}')

    assert buffer_mgr.get_buffer_size(topic) == 5
    dlq_files = list(mock_config.glob("*.json"))
    assert any("buffer_overflow" in f.name for f in dlq_files)


# =====================================================================
# Concurrency & Thread-Safety Tests
# =====================================================================


def test_concurrent_adds(buffer_mgr: BufferManager):
    """Stress test: Validate thread-safety under high concurrent load.

    Simulates multiple MQTT worker threads pushing payloads simultaneously
    to guarantee atomic operations via internal locks.
    """
    buffer_mgr._max_items = 1500
    topic = "factory/stress_topic"

    def worker(thread_id: int):
        for i in range(100):
            buffer_mgr.add_message(topic, f'{{"thread": {thread_id}, "msg": {i}}}')

    with ThreadPoolExecutor(max_workers=10) as executor:
        for t in range(10):
            executor.submit(worker, t)

    assert buffer_mgr.get_buffer_size(topic) == 1000


# =====================================================================
# Retry & Extraction Logic
# =====================================================================


def test_get_ready_topics_timeout(
    buffer_mgr: BufferManager, monkeypatch: pytest.MonkeyPatch
):
    """Verify that topics waiting longer than BATCH_TIMEOUT are flagged as ready."""
    monkeypatch.setattr(Config, "BATCH_TIMEOUT", 0.1)

    buffer_mgr.add_message("factory/timeout_topic", '{"data": 1}')
    assert "factory/timeout_topic" not in buffer_mgr.get_ready_topics()

    time.sleep(0.15)
    assert "factory/timeout_topic" in buffer_mgr.get_ready_topics()


def test_pop_buffer_extraction(buffer_mgr: BufferManager):
    """Verify atomic extraction and queue clearing."""
    topic = "factory/pop_topic"
    buffer_mgr.add_message(topic, '{"id": 1}')

    data = buffer_mgr.pop_buffer(topic)
    assert len(data) == 1
    assert buffer_mgr.get_buffer_size(topic) == 0


def test_restore_buffer_max_retries_dlq(buffer_mgr: BufferManager, mock_config: Path):
    """Verify that failed pipeline batches are retried until the maximum limit is reached."""
    topic = "factory/retries_topic"

    failed_batch = [
        {"payload": {"id": "retry_me"}, "attempts": 1, "received_at": time.time()},
        {"payload": {"id": "discard_me"}, "attempts": 3, "received_at": time.time()},
    ]

    buffer_mgr.restore_buffer(topic, failed_batch)

    # 1. Item with remaining attempts is re-queued
    assert buffer_mgr.get_buffer_size(topic) == 1
    requeued = buffer_mgr.pop_buffer(topic)[0]
    assert requeued["attempts"] == 2

    # 2. Item exceeding attempts goes to DLQ without its metadata wrapper
    dlq_files = list(mock_config.glob("*max_retries_exceeded.json"))
    assert len(dlq_files) == 1

    with open(dlq_files[0], "r") as f:
        data = json.load(f)
        assert data["reason"] == "max_retries_exceeded"
        assert data["data"][0]["id"] == "discard_me"
