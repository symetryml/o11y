"""Modal dialog for selecting a chaos scenario."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Label, OptionList
from textual.widgets.option_list import Option

from otelfl.core.scenarios import SCENARIOS


def _build_options() -> list[Option]:
    options: list[Option] = []
    for key, scenario in SCENARIOS.items():
        flags_str = ", ".join(f"{k}={v}" for k, v in scenario.flags.items())
        prompt = f"{scenario.name}\n  {scenario.description}\n  {flags_str}"
        options.append(Option(prompt, id=key))
    return options


class ScenarioModal(ModalScreen[str | None]):
    """A modal that lets the user pick a scenario from a list."""

    BINDINGS = [("escape", "cancel", "Cancel")]

    def compose(self) -> ComposeResult:
        with Vertical(id="scenario-dialog"):
            yield Label("Select a Chaos Scenario", id="scenario-title")
            yield OptionList(*_build_options(), id="scenario-list")

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        self.dismiss(event.option.id)

    def action_cancel(self) -> None:
        self.dismiss(None)
