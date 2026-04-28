"""Run mode state management."""

from __future__ import annotations

from otelfl.models import RunMode, RUN_MODES, NORMAL_MODE


class RunModeManager:
    """Tracks the active run mode and handles fallback after timed runs."""

    def __init__(self) -> None:
        self.active: RunMode = NORMAL_MODE
        self.fallback: RunMode = NORMAL_MODE
        self._timed_run_active: bool = False
        self._prev_locust_state: str = "unknown"

    def set_mode(self, mode_name: str) -> RunMode:
        """Switch to a named run mode. Updates both active and fallback."""
        mode = RUN_MODES[mode_name]
        self.active = mode
        self.fallback = mode
        self._timed_run_active = False
        return mode

    def start_timed_override(self, users: int, spawn_rate: float) -> None:
        """Begin a temporary override (scenario or manual timed run).

        The current active mode becomes the fallback.
        """
        self.fallback = self.active
        self.active = RunMode(name="override", users=users, spawn_rate=spawn_rate)
        self._timed_run_active = True

    def cancel_timed_run(self) -> None:
        """Cancel pending auto-fallback (e.g. user manually stops)."""
        self._timed_run_active = False

    def check_locust_state(self, new_state: str) -> RunMode | None:
        """Called each poll. Returns a RunMode to restart with if auto-fallback
        should trigger, otherwise None.

        Auto-fallback triggers when:
        - A timed run was active
        - Locust transitions from running/spawning to stopped
        """
        should_fallback = (
            self._timed_run_active
            and self._prev_locust_state in ("running", "spawning")
            and new_state == "stopped"
        )
        self._prev_locust_state = new_state

        if should_fallback:
            self._timed_run_active = False
            self.active = self.fallback
            return self.active
        return None

    @property
    def mode_names(self) -> list[str]:
        return list(RUN_MODES.keys())
