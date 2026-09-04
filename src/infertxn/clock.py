from threading import Lock


class LogicalClock:
    """A process-local monotonic clock shared by all prototype shards."""

    def __init__(self, initial: int = 0) -> None:
        self._value = initial
        self._lock = Lock()

    def now(self) -> int:
        with self._lock:
            return self._value

    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def advance_to(self, minimum: int) -> int:
        """Move the clock forward after replaying durable timestamps."""
        with self._lock:
            self._value = max(self._value, minimum)
            return self._value
