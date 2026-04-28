"""Main CLI entry point using argparse."""

from __future__ import annotations

import argparse
import sys

from rich.console import Console

from otelfl.config import Settings
from otelfl.core.experiment_logger import ExperimentLogger
from otelfl.core.flagd_client import FlagdClient
from otelfl.core.locust_client import LocustClient
from otelfl.cli import (
    flag_commands,
    load_commands,
    stats_commands,
    experiment_commands,
    scenario_commands,
    fetch_commands,
)


# Shared parent parser so --output-format works before or after the subcommand
_common_parser = argparse.ArgumentParser(add_help=False)
_common_parser.add_argument(
    "--output-format",
    "-f",
    choices=["text", "json"],
    default="text",
    help="Output format (default: text)",
)
_common_parser.add_argument("--flagd-url", help="flagd-ui base URL")
_common_parser.add_argument("--locust-url", help="Locust API base URL")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="otelfl",
        description="Control OpenTelemetry Demo feature flags and load generator",
        parents=[_common_parser],
    )
    parser.add_argument(
        "--ts",
        metavar="NAME",
        default=None,
        help="Log timestamped event to NAME.json",
    )

    subparsers = parser.add_subparsers(dest="command")
    flag_commands.register(subparsers, parents=[_common_parser])
    load_commands.register(subparsers, parents=[_common_parser])
    stats_commands.register(subparsers, parents=[_common_parser])
    experiment_commands.register(subparsers, parents=[_common_parser])
    scenario_commands.register(subparsers, parents=[_common_parser])
    fetch_commands.register(subparsers, parents=[_common_parser])
    subparsers.add_parser("tui", help="Launch interactive TUI", parents=[_common_parser])

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        sys.exit(2)

    settings = Settings()
    if args.flagd_url:
        settings.flagd_url = args.flagd_url
    if args.locust_url:
        settings.locust_url = args.locust_url

    output_json = args.output_format == "json"
    console = Console(no_color=output_json, soft_wrap=output_json)

    if args.command == "tui":
        from otelfl.tui.app import OtelFLApp

        app = OtelFLApp(settings=settings)
        app.run()
        return

    code = 0
    if args.command == "flag":
        client = FlagdClient(settings.flagd_url)
        code = flag_commands.run(args, client, console)
    elif args.command in ("load", "stats"):
        client = LocustClient(base_url=settings.locust_url)
        try:
            if args.command == "load":
                code = load_commands.run(args, client, console)
            else:
                code = stats_commands.run(args, client, console)
        finally:
            client.close()
    elif args.command == "scenario":
        client = FlagdClient(settings.flagd_url)
        code = scenario_commands.run(args, client, console)
    elif args.command == "experiment":
        logger = ExperimentLogger()
        code = experiment_commands.run(args, logger, console)
    elif args.command == "fetch":
        code = fetch_commands.run(args, console)

    # --- Timestamp logging ---
    ts_name = getattr(args, "ts", None)
    if ts_name and code == 0:
        from otelfl.core.ts_logger import build_event, append_event

        event = build_event(args)
        if event is not None:
            append_event(ts_name, event, ts_dir=settings.ts_dir)

    sys.exit(code)
