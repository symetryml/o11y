"""Main TUI application using Textual."""

from __future__ import annotations

from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Footer, Header

from otelfl.config import Settings
from otelfl.core.experiment_logger import ExperimentLogger
from otelfl.core.flagd_client import FlagdClient
from otelfl.core.locust_client import AsyncLocustClient, LocustConnectionError
from otelfl.core.run_mode import RunModeManager
from otelfl.core.scenarios import SCENARIOS, apply_scenario
from otelfl.tui.scenario_modal import ScenarioModal
from otelfl.tui.flag_panel import FlagPanel
from otelfl.tui.load_panel import LoadPanel
from otelfl.tui.stats_panel import StatsPanel
from otelfl.tui.timeline_panel import TimelinePanel


class OtelFLApp(App):
    """OpenTelemetry Feature Flag & Load Controller TUI."""

    CSS_PATH = "styles.tcss"

    TITLE = "otelFL"
    SUB_TITLE = "OpenTelemetry Demo Controller"

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "reset_flags", "Reset Flags"),
        Binding("e", "toggle_experiment", "Experiment"),
        Binding("x", "export_experiment", "Export"),
        Binding("i", "import_experiment", "Import"),
        Binding("s", "show_scenarios", "Scenarios"),
        Binding("d", "toggle_endpoint_stats", "Endpoints"),
        Binding("f1", "set_run_mode('low')", "Low", show=False),
        Binding("f2", "set_run_mode('normal')", "Normal", show=False),
        Binding("f3", "set_run_mode('high')", "High", show=False),
        Binding("1", "apply_scenario('mild')", "Mild", show=False),
        Binding("2", "apply_scenario('payment')", "Payment", show=False),
        Binding("3", "apply_scenario('ad-chaos')", "Ad Chaos", show=False),
        Binding("4", "apply_scenario('resource-pressure')", "Resources", show=False),
        Binding("5", "apply_scenario('full-outage')", "Outage", show=False),
        Binding("6", "apply_scenario('llm-issues')", "LLM", show=False),
        Binding("7", "apply_scenario('cascade')", "Cascade", show=False),
    ]

    def __init__(self, settings: Settings | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.settings = settings or Settings()
        self.flagd_client = FlagdClient(self.settings.flagd_url)
        self.locust_client = AsyncLocustClient(base_url=self.settings.locust_url)
        self.experiment_logger = ExperimentLogger()
        self.run_mode_mgr = RunModeManager()

    def compose(self) -> ComposeResult:
        yield Header()
        yield FlagPanel(self.flagd_client, self.experiment_logger)
        yield StatsPanel()
        yield LoadPanel(self.locust_client, self.experiment_logger)
        yield TimelinePanel()
        yield Footer()

    def on_mount(self) -> None:
        self.set_interval(self.settings.poll_interval, self._poll_locust)
        self.log_timeline("experiment", "otelFL started")

    async def _poll_locust(self) -> None:
        stats_panel = self.query_one(StatsPanel)
        load_panel = self.query_one(LoadPanel)
        try:
            stats = await self.locust_client.get_stats()
            stats_panel.update_stats(stats)
            load_panel.update_state(stats.state)
            # Check for auto-fallback after timed run
            restart_mode = self.run_mode_mgr.check_locust_state(stats.state)
            if restart_mode:
                await self.locust_client.start(
                    users=restart_mode.users,
                    spawn_rate=restart_mode.spawn_rate,
                )
                self.log_timeline(
                    "load_change",
                    f"Auto-fallback to {restart_mode.name} mode "
                    f"({restart_mode.users}u, {restart_mode.spawn_rate}/s)",
                )
                load_panel.update_mode(restart_mode.name)
        except LocustConnectionError:
            stats_panel.show_disconnected()
            load_panel.update_state("disconnected")
        except Exception as e:
            stats_panel.show_error(str(e))
            load_panel.update_state("error")

    def log_timeline(self, event_type: str, message: str) -> None:
        try:
            timeline = self.query_one(TimelinePanel)
            timeline.log_event(event_type, message)
        except Exception:
            pass

    def action_reset_flags(self) -> None:
        self.flagd_client.reset_all()
        self.experiment_logger.log_note("All flags reset to off")
        self.log_timeline("flag_change", "All flags reset to off")
        self.query_one(FlagPanel).refresh_flags()
        self.notify("All flags reset")

    def action_toggle_experiment(self) -> None:
        if self.experiment_logger.active:
            exp = self.experiment_logger.stop()
            self.log_timeline("experiment", f"Experiment stopped: {exp.name}")
            self.notify(f"Experiment stopped: {exp.name} ({len(exp.events)} events)")
        else:
            from datetime import datetime, timezone
            ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            name = f"experiment-{ts}"
            exp = self.experiment_logger.start(name)
            self.log_timeline("experiment", f"Experiment started: {exp.name}")
            self.notify(f"Experiment started: {exp.name}")

    def action_export_experiment(self) -> None:
        if not self.experiment_logger.experiment:
            self.notify("No experiment data to export", severity="warning")
            return
        exp = self.experiment_logger.experiment
        path = Path(f"{exp.name}.json")
        self.experiment_logger.export_json(path)
        self.log_timeline("experiment", f"Exported to {path}")
        self.notify(f"Exported to {path}")

    def action_import_experiment(self) -> None:
        """Import flag states from the most recent exported experiment JSON."""
        import glob
        files = sorted(glob.glob("experiment-*.json"), reverse=True)
        if not files:
            self.notify("No experiment-*.json files found", severity="warning")
            return
        path = files[0]
        try:
            snapshot = ExperimentLogger.load_flag_snapshot(path)
            if not snapshot:
                self.notify(f"No flag changes in {path}", severity="warning")
                return
            changes = self.flagd_client.apply_snapshot(snapshot)
            for flag_name, previous, new in changes:
                self.experiment_logger.log_flag_change(flag_name, new, previous)
            self.query_one(FlagPanel).refresh_flags()
            self.log_timeline("experiment", f"Imported {len(changes)} flag(s) from {path}")
            self.notify(f"Imported {len(changes)} flags from {path}")
        except Exception as e:
            self.notify(f"Import error: {e}", severity="error")

    def action_show_scenarios(self) -> None:
        """Open scenario picker modal."""
        self.push_screen(ScenarioModal(), self._on_scenario_selected)

    def _on_scenario_selected(self, scenario_key: str | None) -> None:
        """Callback when a scenario is picked from the modal."""
        if scenario_key is None:
            return
        self.action_apply_scenario(scenario_key)

    def action_set_run_mode(self, mode_name: str) -> None:
        """Switch the active run mode."""
        mode = self.run_mode_mgr.set_mode(mode_name)
        load_panel = self.query_one(LoadPanel)
        load_panel.update_mode(mode.name)
        load_panel.update_fields(mode.users, mode.spawn_rate)
        self.log_timeline("load_change", f"Run mode: {mode}")
        self.notify(f"Run mode: {mode.name}")

    async def action_apply_scenario(self, scenario_key: str) -> None:
        """Apply a named scenario preset."""
        scenario = SCENARIOS.get(scenario_key)
        if not scenario:
            self.notify(f"Unknown scenario: {scenario_key}", severity="error")
            return
        changes = apply_scenario(scenario, self.flagd_client, self.experiment_logger)
        self.query_one(FlagPanel).refresh_flags()
        self.log_timeline("flag_change", f"Scenario applied: {scenario.name}")
        for flag_name, previous, new in changes:
            self.log_timeline("flag_change", f"  {flag_name}: {previous} → {new}")
        # If scenario has load params, start with those
        if scenario.users is not None:
            users = scenario.users
            rate = scenario.spawn_rate or 1.0
            run_time = scenario.run_time
            if run_time:
                self.run_mode_mgr.start_timed_override(users, rate)
            try:
                await self.locust_client.start(
                    users=users, spawn_rate=rate, run_time=run_time
                )
                load_panel = self.query_one(LoadPanel)
                load_panel.update_mode(self.run_mode_mgr.active.name)
                self.log_timeline(
                    "load_change",
                    f"Scenario load: {users}u, {rate}/s"
                    + (f", {run_time}" if run_time else ""),
                )
            except Exception as e:
                self.notify(f"Load error: {e}", severity="error")
        self.notify(f"Applied: {scenario.name} ({len(changes)} flags)")

    def action_toggle_endpoint_stats(self) -> None:
        """Toggle per-endpoint stats display."""
        stats_panel = self.query_one(StatsPanel)
        stats_panel.toggle_endpoints()

    async def action_quit(self) -> None:
        await self.locust_client.close()
        self.exit()
