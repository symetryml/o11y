"""TUI panel for displaying Locust statistics."""

from __future__ import annotations

from datetime import datetime, timezone

from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Static

from otelfl.models import LocustStats


class StatsPanel(VerticalScroll):
    """Panel showing aggregate Locust stats with optional per-endpoint breakdown."""

    def __init__(self, **kwargs) -> None:
        super().__init__(id="stats-panel", **kwargs)
        self.border_title = "Stats"
        self._show_endpoints = False
        self._last_stats: LocustStats | None = None

    def compose(self) -> ComposeResult:
        yield Static("[dim]Waiting for data...[/]", id="stats-content")

    def toggle_endpoints(self) -> None:
        self._show_endpoints = not self._show_endpoints
        if self._last_stats:
            self.update_stats(self._last_stats)

    def update_stats(self, stats: LocustStats) -> None:
        self._last_stats = stats
        content = self.query_one("#stats-content", Static)

        state_style = "green" if stats.state == "running" else "yellow"
        fail_style = "green" if stats.fail_ratio < 0.01 else "red"

        rps_style = "white"
        if stats.total_rps > 100:
            rps_style = "green bold"
        elif stats.total_rps > 0:
            rps_style = "green"

        avg_style = "white"
        if stats.total_avg_response_time > 1000:
            avg_style = "red"
        elif stats.total_avg_response_time > 500:
            avg_style = "yellow"

        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        text = (
            f"[dim]Updated {ts}[/]\n"
            f"State:    [{state_style}]{stats.state}[/]\n"
            f"Users:    {stats.user_count}\n"
            f"RPS:      [{rps_style}]{stats.total_rps:.1f}[/]\n"
            f"Fail:     [{fail_style}]{stats.fail_ratio:.1%}[/]\n"
            f"Avg resp: [{avg_style}]{stats.total_avg_response_time:.0f}ms[/]\n"
            f"Max resp: {stats.total_max_response_time:.0f}ms\n"
            f"Min resp: {stats.total_min_response_time:.0f}ms"
        )

        if stats.errors:
            text += f"\n\n[red]{len(stats.errors)} error(s)[/]"

        if self._show_endpoints and stats.endpoints:
            text += "\n\n[bold]Per-endpoint:[/]\n"
            for ep in stats.endpoints:
                fail_pct = (ep.num_failures / ep.num_requests * 100) if ep.num_requests else 0
                ep_fail_style = "red" if fail_pct > 5 else "green"
                ep_avg_style = "red" if ep.avg_response_time > 1000 else "yellow" if ep.avg_response_time > 500 else "white"
                name = ep.name[:30]
                text += (
                    f"  {ep.method:4s} {name:30s} "
                    f"[{ep_avg_style}]{ep.avg_response_time:6.0f}ms[/] "
                    f"{ep.current_rps:5.1f}rps "
                    f"[{ep_fail_style}]{fail_pct:4.1f}%fail[/]\n"
                )
        elif self._show_endpoints:
            text += "\n\n[dim]No endpoint data yet[/]"

        content.update(text)

    def show_disconnected(self) -> None:
        self._last_stats = None
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        content = self.query_one("#stats-content", Static)
        content.update(f"[dim]{ts}[/]\n[red italic]Locust disconnected[/]")

    def show_error(self, error: str) -> None:
        self._last_stats = None
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        content = self.query_one("#stats-content", Static)
        content.update(f"[dim]{ts}[/]\n[red]Poll error: {error}[/]")
