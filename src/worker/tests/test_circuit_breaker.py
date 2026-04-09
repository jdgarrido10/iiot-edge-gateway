"""
Test suite for CircuitBreaker: stability patterns, fail-fast mechanics, and recovery.
"""

import pytest
import time

from core.circuit_breaker import CircuitBreaker, CircuitState, CircuitOpenError


@pytest.fixture
def cb() -> CircuitBreaker:
    """Instantiate a CircuitBreaker with a low threshold for rapid testing."""
    return CircuitBreaker(failure_threshold=3, timeout=0.5)


def test_circuit_closed_initially(cb: CircuitBreaker):
    """Circuit must initialize in CLOSED state, allowing network calls."""
    assert cb.state == CircuitState.CLOSED


def test_circuit_opens_after_threshold(cb: CircuitBreaker):
    """Circuit must transition to OPEN after consecutive failures, blocking calls."""
    for _ in range(3):
        cb._record_failure()

    assert cb.state == CircuitState.OPEN

    with pytest.raises(CircuitOpenError):
        cb.call(lambda: "should_not_execute")


def test_circuit_recovery_half_open(cb: CircuitBreaker):
    """Circuit must transition to HALF_OPEN after timeout and recover on success."""
    for _ in range(3):
        cb._record_failure()
    assert cb.state == CircuitState.OPEN

    time.sleep(0.6)

    result = cb.call(lambda: "success")

    assert result == "success"
    assert cb.state == CircuitState.CLOSED
    assert cb.failures == 0


def test_circuit_reopens_if_half_open_fails(cb: CircuitBreaker):
    """Circuit must immediately return to OPEN if the HALF_OPEN probe fails."""
    for _ in range(3):
        cb._record_failure()
    time.sleep(0.6)

    def simulated_network_failure():
        raise ValueError("Simulated Timeout")

    with pytest.raises(ValueError):
        cb.call(simulated_network_failure)

    assert cb.state == CircuitState.OPEN


def test_circuit_closed_happy_path(cb: CircuitBreaker):
    """Healthy operations must not increment the failure counter."""
    result = cb.call(lambda: "ok")

    assert result == "ok"
    assert cb.failures == 0
    assert cb.state == CircuitState.CLOSED
