"""TUI panel for managing feature flags."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import VerticalScroll, Horizontal
from textual.widgets import Static, Select

from otelfl.core.flagd_client import FlagdClient, FlagdError
from otelfl.core.experiment_logger import ExperimentLogger
from otelfl.models import FlagDefinition


class BoolFlagLine(Static):
    """Compact [x]/[ ] toggle for boolean/2-variant flags."""

    def __init__(self, flag: FlagDefinition, **kwargs) -> None:
        is_on = flag.default_variant != "off"
        check = "[green]\\[x][/]" if is_on else "[red]\\[ ][/]"
        style = "bold" if is_on else "dim"
        super().__init__(f" {check} [{style}]{flag.name}[/]", **kwargs)
        self.flag_name = flag.name

    def on_click(self) -> None:
        for ancestor in self.ancestors:
            if isinstance(ancestor, FlagPanel):
                ancestor.toggle_bool_flag(self.flag_name)
                return


class MultiFlagRow(Horizontal):
    """Compact row with flag name + dropdown for multi-variant flags."""

    def __init__(self, flag: FlagDefinition, **kwargs) -> None:
        super().__init__(**kwargs)
        self.flag_name = flag.name
        self.flag = flag

    def compose(self) -> ComposeResult:
        is_on = self.flag.default_variant != "off"
        style = "bold" if is_on else "dim"
        yield Static(f" [{style}]{self.flag_name}[/]", classes="multi-flag-label")
        options = [(v, v) for v in self.flag.variant_names]
        yield Select(
            options,
            value=self.flag.default_variant,
            id=f"select-{self.flag_name}",
            name=self.flag_name,
            classes="multi-flag-select",
        )


class FlagPanel(VerticalScroll):
    """Panel displaying all feature flags as compact controls."""

    def __init__(
        self,
        flagd_client: FlagdClient,
        experiment_logger: ExperimentLogger,
        **kwargs,
    ) -> None:
        super().__init__(id="flag-panel", **kwargs)
        self.flagd_client = flagd_client
        self.experiment_logger = experiment_logger
        self.border_title = "Feature Flags"

    def compose(self) -> ComposeResult:
        try:
            flags = self.flagd_client.list_flags()
        except FlagdError as e:
            yield Static(f"[red]Error loading flags: {e}[/]")
            return
        for flag in flags:
            if flag.is_boolean or len(flag.variants) == 2:
                yield BoolFlagLine(flag, classes="flag-line")
            else:
                yield MultiFlagRow(flag, classes="flag-multi")

    def toggle_bool_flag(self, flag_name: str) -> None:
        try:
            flag = self.flagd_client.get_flag(flag_name)
            previous = flag.default_variant
            other = [v for v in flag.variant_names if v != previous][0]
            self.flagd_client.set_flag(flag_name, other)
            self.experiment_logger.log_flag_change(flag_name, other, previous)
            self.app.log_timeline("flag_change", f"{flag_name}: {previous} → {other}")
            self.refresh_flags()
        except FlagdError as e:
            self.notify(f"Error: {e}", severity="error")

    def on_select_changed(self, event: Select.Changed) -> None:
        flag_name = event.select.name
        if not flag_name or event.value is Select.BLANK:
            return
        try:
            flag = self.flagd_client.get_flag(flag_name)
            previous = flag.default_variant
            variant = str(event.value)
            if variant == previous:
                return
            self.flagd_client.set_flag(flag_name, variant)
            self.experiment_logger.log_flag_change(flag_name, variant, previous)
            self.app.log_timeline("flag_change", f"{flag_name}: {previous} → {variant}")
        except FlagdError as e:
            self.notify(f"Error: {e}", severity="error")

    def refresh_flags(self) -> None:
        self.remove_children()
        try:
            flags = self.flagd_client.list_flags()
            for flag in flags:
                if flag.is_boolean or len(flag.variants) == 2:
                    self.mount(BoolFlagLine(flag, classes="flag-line"))
                else:
                    self.mount(MultiFlagRow(flag, classes="flag-multi"))
        except FlagdError as e:
            self.mount(Static(f"[red]Error: {e}[/]"))
