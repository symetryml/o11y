"""Timestamped event logger for CLI commands."""

from __future__ import annotations

import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from otelfl.models import RUN_MODES


def build_event(args) -> dict | None:
    """Build a timestamped event dict from parsed CLI args. Returns None for read-only commands."""
    command = getattr(args, "command", None)
    event = None
    if command == "load":
        event = _build_load_event(args)
    elif command == "flag":
        event = _build_flag_event(args)
    elif command == "scenario":
        event = _build_scenario_event(args)
    if event is not None:
        now = datetime.now(timezone.utc)
        event["ts"] = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
    return event


def _build_load_event(args) -> dict | None:
    action = getattr(args, "load_action", None)
    if action == "start":
        mode = getattr(args, "mode", None)
        users = getattr(args, "users", None)
        rate = getattr(args, "rate", None)
        run_time = getattr(args, "run_time", None)

        if mode:
            rm = RUN_MODES[mode]
            resolved_users = users if users is not None else rm.users
            resolved_rate = rate if rate is not None else rm.spawn_rate
        else:
            resolved_users = users if users is not None else 10
            resolved_rate = rate if rate is not None else 1.0

        anomaly = mode in ("high",) or (mode is None and (users is not None or rate is not None))
        event: dict = {
            "action": "load_start",
            "users": resolved_users,
            "spawn_rate": resolved_rate,
            "anomaly": anomaly,
        }
        if mode:
            event["mode"] = mode
        if run_time:
            event["run_time"] = run_time
        return event
    elif action == "stop":
        return {"action": "load_stop", "anomaly": False}
    return None


def _build_flag_event(args) -> dict | None:
    action = getattr(args, "flag_action", None)
    name = getattr(args, "name", None)

    if action == "enable":
        return {"action": "flag_enable", "flag": name, "anomaly": True}
    elif action == "disable":
        return {"action": "flag_disable", "flag": name, "anomaly": False}
    elif action == "set":
        variant = getattr(args, "variant", None)
        return {"action": "flag_set", "flag": name, "variant": variant, "anomaly": variant != "off"}
    elif action == "toggle":
        return {"action": "flag_toggle", "flag": name, "anomaly": True}
    elif action == "reset":
        return {"action": "flag_reset", "flag": name, "anomaly": False}
    elif action == "restore":
        return {"action": "flag_restore", "anomaly": False}
    elif action == "snapshot":
        return {"action": "flag_snapshot", "anomaly": False}
    return None


def _build_scenario_event(args) -> dict | None:
    action = getattr(args, "scenario_action", None)
    if action == "apply":
        name = getattr(args, "name", None)
        return {"action": "scenario_apply", "scenario": name, "anomaly": True}
    return None


def append_event(ts_name: str, event: dict, ts_dir: Path | None = None) -> None:
    """Append event to {ts_name}.json, creating the file if needed."""
    directory = ts_dir or Path.cwd()
    filepath = Path(directory) / f"{ts_name}.json"
    lockpath = Path(directory) / f"{ts_name}.json.lock"

    with open(lockpath, "w") as lockfile:
        fcntl.flock(lockfile, fcntl.LOCK_EX)
        try:
            if filepath.exists():
                data = json.loads(filepath.read_text())
            else:
                data = {"scenario": ts_name, "events": []}
            data["events"].append(event)

            tmp = filepath.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2) + "\n")
            os.replace(str(tmp), str(filepath))
        finally:
            fcntl.flock(lockfile, fcntl.LOCK_UN)
    lockpath.unlink(missing_ok=True)
