"""CLI load generator subcommands."""

from __future__ import annotations

import argparse
import json

from rich.console import Console

from otelfl.core.locust_client import LocustClient, LocustConnectionError, LocustAPIError
from otelfl.models import RUN_MODES


def register(subparsers: argparse._SubParsersAction, parents: list | None = None) -> None:
    load_parser = subparsers.add_parser("load", help="Control Locust load generator", parents=parents or [])
    load_sub = load_parser.add_subparsers(dest="load_action")

    start_p = load_sub.add_parser("start", help="Start load generation")
    start_p.add_argument(
        "-m", "--mode", choices=list(RUN_MODES.keys()),
        help="Use a named run mode (low, normal, high). Overrides --users and --rate.",
    )
    start_p.add_argument("-u", "--users", type=int, default=None, help="Number of users (default: 10)")
    start_p.add_argument("-r", "--rate", type=float, default=None, help="Spawn rate (default: 1.0)")
    start_p.add_argument("-t", "--run-time", help="Run time (e.g. '5m', '1h')")

    load_sub.add_parser("stop", help="Stop load generation")
    load_sub.add_parser("status", help="Show Locust status")
    load_sub.add_parser("reset-stats", help="Reset Locust statistics")


def run(args: argparse.Namespace, client: LocustClient, console: Console) -> int:
    output_json = getattr(args, "output_format", "text") == "json"

    try:
        if args.load_action == "start":
            return _start(client, console, args, output_json)
        elif args.load_action == "stop":
            return _stop(client, console, output_json)
        elif args.load_action == "status":
            return _status(client, console, output_json)
        elif args.load_action == "reset-stats":
            return _reset_stats(client, console, output_json)
        else:
            console.print("[red]Usage: otelfl load {start,stop,status,reset-stats}[/]")
            return 2
    except LocustConnectionError as e:
        if output_json:
            console.print(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]Connection error:[/] {e}")
        return 1
    except LocustAPIError as e:
        if output_json:
            console.print(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]API error:[/] {e}")
        return 1


def _start(
    client: LocustClient, console: Console, args: argparse.Namespace, output_json: bool
) -> int:
    if args.mode:
        mode = RUN_MODES[args.mode]
        users = args.users if args.users is not None else mode.users
        rate = args.rate if args.rate is not None else mode.spawn_rate
    else:
        users = args.users if args.users is not None else 10
        rate = args.rate if args.rate is not None else 1.0

    result = client.start(users=users, spawn_rate=rate, run_time=args.run_time)
    if output_json:
        console.print(json.dumps(result))
    else:
        mode_label = f" (mode: {args.mode})" if args.mode else ""
        console.print(
            f"[green]Started load generation{mode_label}:[/] {users} users, "
            f"rate={rate}/s"
            + (f", run_time={args.run_time}" if args.run_time else "")
        )
    return 0


def _stop(client: LocustClient, console: Console, output_json: bool) -> int:
    result = client.stop()
    if output_json:
        console.print(json.dumps(result) if isinstance(result, dict) else json.dumps({"status": "stopped"}))
    else:
        console.print("[yellow]Load generation stopped[/]")
    return 0


def _status(client: LocustClient, console: Console, output_json: bool) -> int:
    stats = client.get_stats()
    if output_json:
        console.print(json.dumps({
            "state": stats.state, "users": stats.user_count,
            "rps": stats.total_rps, "fail_ratio": stats.fail_ratio,
            "avg_response_time": stats.total_avg_response_time,
        }))
    else:
        state_color = "green" if stats.state == "running" else "yellow"
        console.print(f"State: [{state_color}]{stats.state}[/]")
        console.print(f"Users: {stats.user_count}")
        console.print(f"RPS: {stats.total_rps:.1f}")
        console.print(f"Fail ratio: {stats.fail_ratio:.1%}")
        console.print(f"Avg response: {stats.total_avg_response_time:.0f}ms")
    return 0


def _reset_stats(client: LocustClient, console: Console, output_json: bool) -> int:
    client.reset_stats()
    if output_json:
        console.print(json.dumps({"status": "reset"}))
    else:
        console.print("[green]Stats reset[/]")
    return 0
