"""TUI panel for controlling the Locust load generator."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.widgets import Static, Button, Input, Label, Select

from otelfl.core.locust_client import AsyncLocustClient, LocustConnectionError
from otelfl.core.experiment_logger import ExperimentLogger
from otelfl.models import RUN_MODES


class LoadPanel(Vertical):
    """Panel for starting/stopping Locust and configuring load parameters."""

    def __init__(
        self,
        locust_client: AsyncLocustClient,
        experiment_logger: ExperimentLogger,
        **kwargs,
    ) -> None:
        super().__init__(id="load-panel", **kwargs)
        self.locust_client = locust_client
        self.experiment_logger = experiment_logger
        self.border_title = "Load Generator"

    def compose(self) -> ComposeResult:
        with Horizontal(id="load-status-row"):
            yield Static("", id="load-state-badge")
            yield Static("[dim]Mode: normal[/]", id="load-mode-label")
        mode_options = [(name.capitalize(), name) for name in RUN_MODES]
        yield Select(
            mode_options,
            value="normal",
            id="load-mode-select",
            prompt="Run Mode",
        )
        with Vertical(id="load-fields"):
            yield Label("[bold]Users[/] [dim]concurrent users hitting the demo[/]")
            yield Input(placeholder="Users", value="5", id="load-users")
            yield Label("[bold]Rate[/] [dim]users added per second during ramp-up[/]")
            yield Input(placeholder="Rate", value="1", id="load-rate")
            yield Label("[bold]Run time[/] [dim]e.g. 5m, 1h, 30s (empty = forever)[/]")
            yield Input(placeholder="Run time", value="", id="load-runtime")
        with Horizontal(id="load-buttons"):
            yield Button("Start", id="load-start", variant="success")
            yield Button("Stop", id="load-stop", variant="warning")
            yield Button("Reset Stats", id="load-reset", variant="default")

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id != "load-mode-select":
            return
        if event.value is Select.BLANK:
            return
        mode_name = str(event.value)
        self.app.action_set_run_mode(mode_name)

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        try:
            if event.button.id == "load-start":
                users_input = self.query_one("#load-users", Input)
                rate_input = self.query_one("#load-rate", Input)
                runtime_input = self.query_one("#load-runtime", Input)
                users = int(users_input.value or "5")
                rate = float(rate_input.value or "1")
                runtime = runtime_input.value or None
                await self.locust_client.start(users=users, spawn_rate=rate, run_time=runtime)
                self.experiment_logger.log_load_change(
                    "start", users=users, rate=rate, run_time=runtime
                )
                self.app.log_timeline(
                    "load_change", f"Started: {users} users, rate={rate}/s"
                )
                # Track timed runs for auto-fallback
                if runtime:
                    self.app.run_mode_mgr.start_timed_override(users, rate)
                self.notify("Load generation started")

            elif event.button.id == "load-stop":
                await self.locust_client.stop()
                self.experiment_logger.log_load_change("stop")
                self.app.log_timeline("load_change", "Stopped load generation")
                # Cancel auto-fallback on manual stop
                self.app.run_mode_mgr.cancel_timed_run()
                self.notify("Load generation stopped")

            elif event.button.id == "load-reset":
                await self.locust_client.reset_stats()
                self.experiment_logger.log_load_change("reset_stats")
                self.app.log_timeline("load_change", "Stats reset")
                self.notify("Stats reset")

        except LocustConnectionError as e:
            self.notify(f"Cannot connect to Locust: {e}", severity="error")
            self.app.log_timeline("error", f"Locust connection error: {e}")
        except Exception as e:
            self.notify(f"Error: {e}", severity="error")
            self.app.log_timeline("error", f"Load error: {e}")

    def update_state(self, state: str) -> None:
        badge = self.query_one("#load-state-badge", Static)
        if state == "running":
            badge.update("[bold green]● RUNNING[/]")
        elif state == "stopped":
            badge.update("[bold yellow]● STOPPED[/]")
        else:
            badge.update(f"[dim italic]● {state}[/]")

    def update_mode(self, mode_name: str) -> None:
        """Update the displayed mode name."""
        label = self.query_one("#load-mode-label", Static)
        label.update(f"[bold cyan]Mode: {mode_name}[/]")

    def update_fields(self, users: int, spawn_rate: float) -> None:
        """Update the input field values to match a mode."""
        self.query_one("#load-users", Input).value = str(users)
        self.query_one("#load-rate", Input).value = str(spawn_rate)
