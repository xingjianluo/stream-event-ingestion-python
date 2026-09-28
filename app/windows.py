"""Event-time tumbling windows with watermark and late-event handling."""
from __future__ import annotations

from .models import Event
from .storage import Storage


class WindowEngine:
    """Tumbling windows; lateness decided by a per-source watermark.

    watermark(source) = max event_time seen so far - allowed_lateness.

    Stable rules (never silently drop, never corrupt closed windows):
    - event whose window is still open  -> window.count += 1
    - event whose window is already closed -> window.late_count += 1
      (recorded explicitly and queryable; the closed count stays frozen)
    """

    def __init__(self, storage: Storage, window_size_ms: int, allowed_lateness_ms: int) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be positive")
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be >= 0")
        self._storage = storage
        self._window_size_ms = window_size_ms
        self._allowed_lateness_ms = allowed_lateness_ms

    @property
    def window_size_ms(self) -> int:
        return self._window_size_ms

    def window_bounds(self, event_time: int) -> tuple[int, int]:
        """Return [start, end) of the window containing event_time."""
        start = (event_time // self._window_size_ms) * self._window_size_ms
        return start, start + self._window_size_ms

    def on_event(self, event: Event) -> None:
        """Update watermark, close expired windows, count the event.

        Must be called inside the same storage transaction as the log
        append so a crash never leaves the log and aggregates diverged.
        """
        source = event.source
        candidate = event.event_time - self._allowed_lateness_ms
        self._storage.advance_watermark(source, candidate)
        watermark = self._storage.get_watermark(source)
        assert watermark is not None
        self._storage.close_windows_before(source, watermark)

        start, end = self.window_bounds(event.event_time)
        # Late iff the watermark has already passed this window's end —
        # independent of whether a row for the window exists yet.
        late = end <= watermark
        self._storage.add_to_window(event, start, end, late)
        # A brand-new window created by a very late event may already be
        # beyond the watermark; close it immediately so its state is stable.
        self._storage.close_windows_before(source, watermark)
