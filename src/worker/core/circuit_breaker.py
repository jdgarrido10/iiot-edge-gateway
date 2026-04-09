"""
Circuit Breaker pattern implementation for protecting external service calls.
"""

import time
import threading
from enum import Enum

from core.logger import setup_logger

logger = setup_logger(__name__)


class CircuitState(Enum):
    CLOSED = "CLOSED"  # Normal operation — calls allowed.
    OPEN = "OPEN"  # Service down — calls rejected immediately (fast-fail).
    HALF_OPEN = "HALF_OPEN"  # Probing recovery — one call allowed through.


class CircuitBreaker:
    """Thread-safe implementation of the Circuit Breaker pattern.

    State machine:
    - **CLOSED** → normal operation.
    - **OPEN** → all calls raise :class:`CircuitOpenError` until ``timeout``
      seconds have elapsed since the last failure.
    - **HALF_OPEN** → a single call is attempted; success resets to CLOSED,
      failure returns to OPEN.

    Args:
        failure_threshold: Consecutive failures required to open the circuit.
        timeout: Seconds to wait in OPEN state before attempting HALF_OPEN.
    """

    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self._lock = threading.Lock()
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = 0
        self.state = CircuitState.CLOSED
        self.last_failure_time: float | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def call(self, func, *args, **kwargs):
        """Execute *func* protected by the circuit breaker.

        Args:
            func: Callable to invoke.
            *args: Positional arguments forwarded to *func*.
            **kwargs: Keyword arguments forwarded to *func*.

        Returns:
            The return value of *func*.

        Raises:
            CircuitOpenError: When the circuit is OPEN and the retry
                timeout has not yet elapsed.
            Exception: Any exception raised by *func* itself.
        """
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    logger.info("Circuit HALF_OPEN — probing recovery")
                else:
                    raise CircuitOpenError(
                        f"Circuit OPEN — service unavailable (failures: {self.failures})"
                    )

        try:
            result = func(*args, **kwargs)

            with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self._reset_unsafe()
                    logger.info("Circuit CLOSED — service recovered")

            return result

        except Exception:
            self._record_failure()
            raise

    def reset(self) -> None:
        """Manually reset the circuit to CLOSED state (thread-safe).

        Useful for testing or operator-triggered recovery.
        """
        with self._lock:
            self._reset_unsafe()
        logger.info("Circuit reset manually")

    def get_state(self) -> dict:
        """Return a snapshot of the current circuit state (thread-safe).

        Returns:
            Dict with keys ``state``, ``failures``, ``threshold``,
            and ``last_failure``.
        """
        with self._lock:
            return {
                "state": self.state.value,
                "failures": self.failures,
                "threshold": self.failure_threshold,
                "last_failure": self.last_failure_time,
            }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _record_failure(self) -> None:
        """Increment failure counter and open the circuit if threshold is reached.

        Must be called without holding ``_lock`` (acquires it internally).
        """
        with self._lock:
            self.failures += 1
            self.last_failure_time = time.time()

            if self.failures >= self.failure_threshold:
                self.state = CircuitState.OPEN
                logger.error(
                    f"Circuit OPEN — threshold reached "
                    f"({self.failures}/{self.failure_threshold})"
                )

    def _should_attempt_reset(self) -> bool:
        """Return True if enough time has elapsed to attempt a reset.

        Must be called with ``_lock`` already held.
        """
        if self.last_failure_time is None:
            return True
        return (time.time() - self.last_failure_time) >= self.timeout

    def _reset_unsafe(self) -> None:
        """Reset counters and state to CLOSED.

        Must be called with ``_lock`` already held.
        """
        self.failures = 0
        self.state = CircuitState.CLOSED
        self.last_failure_time = None


class CircuitOpenError(Exception):
    """Raised when a call is attempted while the circuit is in OPEN state."""
