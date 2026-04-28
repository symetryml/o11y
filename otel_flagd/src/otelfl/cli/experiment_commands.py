"""CLI experiment subcommands."""

from __future__ import annotations

import argparse
import json

from rich.console import Console

from otelfl.core.experiment_logger import ExperimentLogger


def register(subparsers: argparse._SubParsersAction, parents: list | None = None) -> None:
    exp_parser = subparsers.add_parser("experiment", help="Manage experiments", parents=parents or [])
    exp_sub = exp_parser.add_subparsers(dest="exp_action")

    start_p = exp_sub.add_parser("start", help="Start an experiment")
    start_p.add_argument("name", help="Experiment name")

    exp_sub.add_parser("stop", help="Stop current experiment")

    export_p = exp_sub.add_parser("export", help="Export experiment data")
    export_p.add_argument("path", help="Output file path (.json or .csv)")


# Note: The experiment logger is shared state. In CLI mode each command is a
# separate invocation, so experiment state only persists within a single process
# (i.e., primarily useful in TUI mode). The CLI commands are provided for
# scripting workflows where the logger is passed through.

def run(args: argparse.Namespace, logger: ExperimentLogger, console: Console) -> int:
    output_json = getattr(args, "output_format", "text") == "json"

    if args.exp_action == "start":
        exp = logger.start(args.name)
        if output_json:
            console.print(json.dumps({"experiment": exp.name, "started": exp.started_at.isoformat()}))
        else:
            console.print(f"[green]Experiment started:[/] {exp.name}")
        return 0

    elif args.exp_action == "stop":
        exp = logger.stop()
        if exp:
            if output_json:
                console.print(json.dumps({"experiment": exp.name, "events": len(exp.events)}))
            else:
                console.print(f"[yellow]Experiment stopped:[/] {exp.name} ({len(exp.events)} events)")
        else:
            console.print("[dim]No active experiment[/]")
        return 0

    elif args.exp_action == "export":
        if not logger.experiment:
            console.print("[red]No experiment data to export[/]")
            return 1
        path = args.path
        if path.endswith(".csv"):
            logger.export_csv(path)
        else:
            logger.export_json(path)
        if output_json:
            console.print(json.dumps({"exported": path}))
        else:
            console.print(f"[green]Exported to:[/] {path}")
        return 0

    else:
        console.print("[red]Usage: otelfl experiment {start,stop,export}[/]")
        return 2
