"""CLI scenario subcommands."""

from __future__ import annotations

import argparse
import json

from rich.console import Console
from rich.table import Table

from otelfl.core.flagd_client import FlagdClient, FlagdError
from otelfl.core.scenarios import SCENARIOS, apply_scenario


def register(subparsers: argparse._SubParsersAction, parents: list | None = None) -> None:
    sc_parser = subparsers.add_parser("scenario", help="Apply chaos scenario presets", parents=parents or [])
    sc_sub = sc_parser.add_subparsers(dest="scenario_action")

    sc_sub.add_parser("list", help="List available scenarios")

    apply_p = sc_sub.add_parser("apply", help="Apply a scenario (interactive picker if no name given)")
    apply_p.add_argument("name", nargs="?", default=None, help="Scenario name (omit for interactive picker)")


def run(args: argparse.Namespace, client: FlagdClient, console: Console) -> int:
    output_json = getattr(args, "output_format", "text") == "json"

    try:
        if args.scenario_action == "list":
            return _list_scenarios(console, output_json)
        elif args.scenario_action == "apply":
            name = args.name
            if name is None:
                name = _pick_scenario(console)
                if name is None:
                    return 0
            if name not in SCENARIOS:
                console.print(f"[red]Unknown scenario:[/] {name}")
                console.print(f"Available: {', '.join(SCENARIOS.keys())}")
                return 1
            return _apply_scenario(client, console, name, output_json)
        else:
            # No subcommand — show interactive picker
            name = _pick_scenario(console)
            if name is None:
                return 0
            return _apply_scenario(client, console, name, output_json)
    except FlagdError as e:
        if output_json:
            console.print(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]Error:[/] {e}")
        return 1


def _pick_scenario(console: Console) -> str | None:
    """Interactive scenario picker."""
    scenarios = list(SCENARIOS.items())
    console.print("\n[bold]Select a Chaos Scenario:[/]\n")
    for i, (key, s) in enumerate(scenarios, 1):
        flags_str = ", ".join(f"{k}={v}" for k, v in s.flags.items())
        console.print(f"  [cyan]{i}[/]) [bold]{s.name}[/]")
        console.print(f"     {s.description}")
        console.print(f"     [dim]{flags_str}[/]")
    console.print(f"\n  [dim]0) Cancel[/]\n")

    try:
        choice = input("Enter number: ").strip()
    except (EOFError, KeyboardInterrupt):
        return None

    if not choice or choice == "0":
        return None

    try:
        idx = int(choice) - 1
        if 0 <= idx < len(scenarios):
            return scenarios[idx][0]
    except ValueError:
        # Try matching by name
        if choice in SCENARIOS:
            return choice

    console.print(f"[red]Invalid choice: {choice}[/]")
    return None


def _list_scenarios(console: Console, output_json: bool) -> int:
    if output_json:
        console.print(json.dumps([{
            "key": key, "name": s.name,
            "description": s.description, "flags": s.flags,
        } for key, s in SCENARIOS.items()], indent=2))
        return 0

    table = Table(title="Chaos Scenarios")
    table.add_column("Key", style="cyan")
    table.add_column("Name", style="bold")
    table.add_column("Description")
    table.add_column("Flags")
    for key, s in SCENARIOS.items():
        flags_str = ", ".join(f"{k}={v}" for k, v in s.flags.items())
        table.add_row(key, s.name, s.description, flags_str)
    console.print(table)
    return 0


def _apply_scenario(
    client: FlagdClient, console: Console, name: str, output_json: bool
) -> int:
    scenario = SCENARIOS[name]
    changes = apply_scenario(scenario, client)
    if output_json:
        console.print(json.dumps({
            "scenario": name, "applied": len(changes),
            "changes": [{"flag": f, "previous": p, "current": n} for f, p, n in changes],
        }))
    else:
        console.print(f"[green]Applied scenario:[/] [bold]{scenario.name}[/]")
        console.print(f"  {scenario.description}")
        for flag_name, previous, new in changes:
            console.print(f"  [cyan]{flag_name}[/]: {previous} → [bold]{new}[/]")
    return 0
