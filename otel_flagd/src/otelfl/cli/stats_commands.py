"""CLI stats subcommand."""

from __future__ import annotations

import argparse
import json

from rich.console import Console
from rich.table import Table

from otelfl.core.locust_client import LocustClient, LocustConnectionError, LocustAPIError


def register(subparsers: argparse._SubParsersAction, parents: list | None = None) -> None:
    subparsers.add_parser("stats", help="Show Locust stats summary", parents=parents or [])


def run(args: argparse.Namespace, client: LocustClient, console: Console) -> int:
    output_json = getattr(args, "output_format", "text") == "json"
    try:
        stats = client.get_stats()
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

    if output_json:
        console.print(json.dumps({
            "state": stats.state,
            "user_count": stats.user_count,
            "total_rps": stats.total_rps,
            "fail_ratio": stats.fail_ratio,
            "avg_response_time": stats.total_avg_response_time,
            "max_response_time": stats.total_max_response_time,
            "min_response_time": stats.total_min_response_time,
            "errors": stats.errors,
            "endpoints": [{
                "name": ep.name, "method": ep.method,
                "requests": ep.num_requests, "failures": ep.num_failures,
                "rps": ep.current_rps, "avg_ms": ep.avg_response_time,
                "max_ms": ep.max_response_time,
            } for ep in stats.endpoints],
        }, indent=2))
        return 0

    # Aggregate stats table
    table = Table(title="Locust Stats")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="bold")

    state_color = "green" if stats.state == "running" else "yellow"
    table.add_row("State", f"[{state_color}]{stats.state}[/]")
    table.add_row("Users", str(stats.user_count))
    table.add_row("RPS", f"{stats.total_rps:.1f}")

    fail_color = "green" if stats.fail_ratio < 0.01 else "red"
    table.add_row("Fail ratio", f"[{fail_color}]{stats.fail_ratio:.1%}[/]")
    table.add_row("Avg response", f"{stats.total_avg_response_time:.0f}ms")
    table.add_row("Max response", f"{stats.total_max_response_time:.0f}ms")
    table.add_row("Min response", f"{stats.total_min_response_time:.0f}ms")

    console.print(table)

    # Per-endpoint table
    if stats.endpoints:
        ep_table = Table(title="Per-Endpoint Stats")
        ep_table.add_column("Method", style="dim")
        ep_table.add_column("Endpoint", style="cyan")
        ep_table.add_column("Reqs", justify="right")
        ep_table.add_column("Fails", justify="right")
        ep_table.add_column("RPS", justify="right")
        ep_table.add_column("Avg ms", justify="right")
        ep_table.add_column("Max ms", justify="right")
        for ep in stats.endpoints:
            fail_pct = (ep.num_failures / ep.num_requests * 100) if ep.num_requests else 0
            fail_style = "red" if fail_pct > 5 else ""
            avg_style = "red" if ep.avg_response_time > 1000 else "yellow" if ep.avg_response_time > 500 else ""
            ep_table.add_row(
                ep.method,
                ep.name,
                str(ep.num_requests),
                f"[{fail_style}]{ep.num_failures}[/]" if fail_style else str(ep.num_failures),
                f"{ep.current_rps:.1f}",
                f"[{avg_style}]{ep.avg_response_time:.0f}[/]" if avg_style else f"{ep.avg_response_time:.0f}",
                f"{ep.max_response_time:.0f}",
            )
        console.print(ep_table)

    if stats.errors:
        console.print(f"\n[red]{len(stats.errors)} error(s)[/]")
        for err in stats.errors:
            console.print(f"  {err.get('method')} {err.get('name')}: {err.get('occurrences')}x")

    return 0
