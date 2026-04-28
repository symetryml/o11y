"""CLI flag subcommands."""

from __future__ import annotations

import argparse
import json

from rich.console import Console
from rich.table import Table

from otelfl.core.flagd_client import FlagdClient, FlagdError


def register(subparsers: argparse._SubParsersAction, parents: list | None = None) -> None:
    flag_parser = subparsers.add_parser("flag", help="Manage feature flags", parents=parents or [])
    flag_sub = flag_parser.add_subparsers(dest="flag_action")

    flag_sub.add_parser("list", help="List all flags")

    get_p = flag_sub.add_parser("get", help="Get a flag's details")
    get_p.add_argument("name", help="Flag name")

    set_p = flag_sub.add_parser("set", help="Set a flag's variant")
    set_p.add_argument("name", help="Flag name")
    set_p.add_argument("variant", help="Variant to set")

    toggle_p = flag_sub.add_parser("toggle", help="Toggle a boolean flag")
    toggle_p.add_argument("name", help="Flag name")

    reset_p = flag_sub.add_parser("reset", help="Reset a flag to off")
    reset_p.add_argument("name", help="Flag name (or 'all' to reset all)")

    enable_p = flag_sub.add_parser("enable", help="Enable a flag")
    enable_p.add_argument("name", help="Flag name")

    disable_p = flag_sub.add_parser("disable", help="Disable a flag")
    disable_p.add_argument("name", help="Flag name")

    snap_p = flag_sub.add_parser("snapshot", help="Save current flag states to JSON")
    snap_p.add_argument("path", help="Output file path")

    restore_p = flag_sub.add_parser("restore", help="Restore flag states from JSON snapshot")
    restore_p.add_argument("path", help="Snapshot or experiment JSON file")


def run(args: argparse.Namespace, client: FlagdClient, console: Console) -> int:
    output_json = getattr(args, "output_format", "text") == "json"

    try:
        if args.flag_action == "list":
            return _list_flags(client, console, output_json)
        elif args.flag_action == "get":
            return _get_flag(client, console, args.name, output_json)
        elif args.flag_action == "set":
            return _set_flag(client, console, args.name, args.variant, output_json)
        elif args.flag_action == "toggle":
            return _toggle_flag(client, console, args.name, output_json)
        elif args.flag_action == "reset":
            return _reset_flag(client, console, args.name, output_json)
        elif args.flag_action == "enable":
            return _set_state(client, console, args.name, "ENABLED", output_json)
        elif args.flag_action == "disable":
            return _set_state(client, console, args.name, "DISABLED", output_json)
        elif args.flag_action == "snapshot":
            return _snapshot(client, console, args.path, output_json)
        elif args.flag_action == "restore":
            return _restore(client, console, args.path, output_json)
        else:
            console.print("[red]Usage: otelfl flag {list,get,set,toggle,reset,enable,disable,snapshot,restore}[/]")
            return 2
    except FlagdError as e:
        if output_json:
            console.print(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]Error:[/] {e}")
        return 1


def _list_flags(client: FlagdClient, console: Console, output_json: bool) -> int:
    flags = client.list_flags()
    if output_json:
        console.print(json.dumps([{
            "name": f.name, "type": f.variant_type, "state": f.state,
            "default": f.default_variant, "variants": f.variant_names,
            "description": f.description,
        } for f in flags], indent=2))
        return 0

    table = Table(title="Feature Flags")
    table.add_column("Flag", style="cyan")
    table.add_column("State", style="dim")
    table.add_column("Type", style="dim")
    table.add_column("Current", style="bold")
    table.add_column("Variants")
    table.add_column("Description", style="dim", max_width=40)
    for f in flags:
        current_style = "green" if f.default_variant == "off" else "red bold"
        state_style = "green" if f.state == "ENABLED" else "red"
        table.add_row(
            f.name,
            f"[{state_style}]{f.state}[/]",
            f.variant_type,
            f"[{current_style}]{f.default_variant}[/]",
            ", ".join(f.variant_names),
            f.description,
        )
    console.print(table)
    return 0


def _get_flag(client: FlagdClient, console: Console, name: str, output_json: bool) -> int:
    flag = client.get_flag(name)
    if output_json:
        console.print(json.dumps({
            "name": flag.name, "type": flag.variant_type,
            "default": flag.default_variant, "value": flag.current_value,
            "variants": flag.variants, "description": flag.description,
        }, indent=2))
        return 0

    console.print(f"[cyan]{flag.name}[/] ({flag.variant_type})")
    console.print(f"  Description: {flag.description}")
    console.print(f"  Current: [bold]{flag.default_variant}[/] = {flag.current_value}")
    console.print(f"  Variants: {flag.variants}")
    return 0


def _set_flag(
    client: FlagdClient, console: Console, name: str, variant: str, output_json: bool
) -> int:
    flag = client.get_flag(name)
    previous = flag.default_variant
    flag = client.set_flag(name, variant)
    if output_json:
        console.print(json.dumps({"name": name, "previous": previous, "current": variant}))
    else:
        console.print(f"[cyan]{name}[/]: {previous} → [bold]{variant}[/]")
    return 0


def _toggle_flag(client: FlagdClient, console: Console, name: str, output_json: bool) -> int:
    previous = client.get_flag(name).default_variant
    flag = client.toggle_flag(name)
    if output_json:
        console.print(json.dumps({
            "name": name, "previous": previous, "current": flag.default_variant,
        }))
    else:
        console.print(f"[cyan]{name}[/]: {previous} → [bold]{flag.default_variant}[/]")
    return 0


def _reset_flag(client: FlagdClient, console: Console, name: str, output_json: bool) -> int:
    if name == "all":
        flags = client.reset_all()
        if output_json:
            console.print(json.dumps({"reset": [f.name for f in flags]}))
        else:
            console.print(f"[green]Reset all {len(flags)} flags to off[/]")
    else:
        flag = client.reset_flag(name)
        if output_json:
            console.print(json.dumps({"name": name, "current": flag.default_variant}))
        else:
            console.print(f"[green]Reset {name} → off[/]")
    return 0


def _set_state(
    client: FlagdClient, console: Console, name: str, state: str, output_json: bool
) -> int:
    flag = client.set_flag_state(name, state)
    if output_json:
        console.print(json.dumps({"name": name, "state": flag.state}))
    else:
        style = "green" if state == "ENABLED" else "red"
        console.print(f"[cyan]{name}[/] → [{style}]{flag.state}[/]")
    return 0


def _snapshot(client: FlagdClient, console: Console, path: str, output_json: bool) -> int:
    snapshot = client.get_snapshot()
    from pathlib import Path
    Path(path).write_text(json.dumps(snapshot, indent=2) + "\n")
    if output_json:
        console.print(json.dumps({"snapshot": path, "flags": len(snapshot)}))
    else:
        console.print(f"[green]Saved snapshot of {len(snapshot)} flags to {path}[/]")
    return 0


def _restore(client: FlagdClient, console: Console, path: str, output_json: bool) -> int:
    from pathlib import Path
    data = json.loads(Path(path).read_text())
    # Support both raw snapshot ({flag: variant}) and experiment export format
    if "events" in data:
        from otelfl.core.experiment_logger import ExperimentLogger
        snapshot = ExperimentLogger.load_flag_snapshot(path)
    else:
        snapshot = data
    changes = client.apply_snapshot(snapshot)
    if output_json:
        console.print(json.dumps({"restored": len(changes), "changes": [
            {"flag": f, "previous": p, "current": n} for f, p, n in changes
        ]}))
    else:
        if changes:
            console.print(f"[green]Restored {len(changes)} flag(s):[/]")
            for flag_name, previous, new in changes:
                console.print(f"  [cyan]{flag_name}[/]: {previous} → [bold]{new}[/]")
        else:
            console.print("[dim]No changes needed[/]")
    return 0
