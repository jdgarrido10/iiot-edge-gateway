"""
Test suite for StateManager: atomic checkpoints, deduplication, and rollover logic.
"""

import pytest
import pandas as pd
import json
from pathlib import Path

from core.state_manager import StateManager
from config import Config

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def state_mgr(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> StateManager:
    """Create a StateManager with isolated temp directories for checkpoints."""
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    chk_file = test_dir / "checkpoint.json"

    monkeypatch.setattr(Config, "DATA_DIR", test_dir)
    monkeypatch.setattr(Config, "CHECKPOINT_FILE", chk_file)
    monkeypatch.setattr(Config, "ROLLOVER_THRESHOLD", 500000)

    return StateManager()


# =====================================================================
# Tests
# =====================================================================


def test_initial_state_is_empty(state_mgr: StateManager):
    """A fresh instance without a checkpoint file must start empty."""
    assert state_mgr.state == {}


def test_save_checkpoint_updates_memory_and_disk(state_mgr: StateManager):
    """Verify that checkpoints update both runtime memory and disk files."""
    topic = "factory/packaging/line_1"
    df = pd.DataFrame(
        [
            {"identity": 500, "timestamp": "2024-01-01T10:00:00Z"},
            {"identity": 510, "timestamp": "2024-01-01T10:05:00Z"},
        ]
    )

    state_mgr.save_checkpoint(df, topic)

    assert state_mgr.state[topic]["Identity"] == 510
    assert Config.CHECKPOINT_FILE.exists()

    with open(Config.CHECKPOINT_FILE, "r") as f:
        data = json.load(f)
        assert data[topic]["Identity"] == 510


def test_is_already_processed_logic(state_mgr: StateManager):
    """Verify core deduplication: block older/equal identities, allow newer."""
    topic = "factory/packaging/line_2"
    state_mgr.state[topic] = {"Identity": 1000}

    assert state_mgr.is_already_processed(999, topic) is True
    assert state_mgr.is_already_processed(1000, topic) is True
    assert state_mgr.is_already_processed(1001, topic) is False


def test_rollover_detection(state_mgr: StateManager):
    """Hardware edge case: PLCs reset their counters to 0 upon reboot.

    The StateManager must detect large negative deltas and allow them as rollovers.
    """
    topic = "factory/packaging/line_3"
    state_mgr.state[topic] = {"Identity": 999999}

    # 1 is significantly lower than 999999 (exceeds ROLLOVER_THRESHOLD)
    # Must be evaluated as a valid new record, not a duplicate.
    assert state_mgr.is_already_processed(1, topic) is False


def test_is_already_processed_with_string_identity(state_mgr: StateManager):
    """Ensure type coercion works if JSON parsing leaves identities as strings."""
    topic = "factory/packaging/string_topic"
    state_mgr.state[topic] = {"Identity": 100}

    assert state_mgr.is_already_processed("100", topic) is True
    assert state_mgr.is_already_processed("101", topic) is False


def test_get_state_summary(state_mgr: StateManager):
    """Verify HTTP API diagnostic output generation."""
    topic = "factory/packaging/api_topic"
    df = pd.DataFrame([{"identity": 404, "timestamp": "2024-01-01T10:00:00Z"}])
    state_mgr.save_checkpoint(df, topic)

    summary = state_mgr.get_state_summary()

    assert summary["topics_tracked"] == 1
    assert topic in summary["topics"]
    assert summary["topics"][topic]["last_identity"] == 404
