"""TUI panel for displaying experiment timeline events."""

from __future__ import annotations

from datetime import datetime, timezone

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import RichLog


EVENT_COLORS = {
    "flag_change": "cyan",
    "load_change": "yellow",
    "note": "white",
    "experiment": "magenta",
    "error": "red",
}


class TimelinePanel(Vertical):
    """Panel showing a scrolling log of timestamped experiment events."""

    def __init__(self, **kwargs) -> None:
        super().__init__(id="timeline-panel", **kwargs)
        self.border_title = "Timeline"

    def compose(self) -> ComposeResult:
        yield RichLog(id="timeline-log", wrap=True, markup=True)

    def log_event(self, event_type: str, message: str) -> None:
        log = self.query_one("#timeline-log", RichLog)
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        color = EVENT_COLORS.get(event_type, "white")
        log.write(f"[dim]{ts}[/] [{color}]{message}[/]")
