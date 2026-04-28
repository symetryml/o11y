"""In-memory experiment event recording with export."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from otelfl.models import Experiment, ExperimentEvent


class ExperimentLogger:
    """Records timestamped events during an experiment session."""

    def __init__(self) -> None:
        self._experiment: Experiment | None = None

    @property
    def active(self) -> bool:
        return self._experiment is not None and self._experiment.stopped_at is None

    @property
    def experiment(self) -> Experiment | None:
        return self._experiment

    def start(self, name: str) -> Experiment:
        self._experiment = Experiment(
            name=name,
            started_at=datetime.now(timezone.utc),
        )
        return self._experiment

    def stop(self) -> Experiment | None:
        if self._experiment and self._experiment.stopped_at is None:
            self._experiment.stopped_at = datetime.now(timezone.utc)
        return self._experiment

    def log_event(self, event_type: str, details: dict[str, Any]) -> ExperimentEvent | None:
        """Log an event. No-op if no experiment is active."""
        if not self.active:
            return None
        event = ExperimentEvent(
            timestamp=datetime.now(timezone.utc),
            event_type=event_type,
            details=details,
        )
        self._experiment.events.append(event)
        return event

    def log_flag_change(self, flag_name: str, variant: str, previous: str) -> ExperimentEvent | None:
        return self.log_event(
            "flag_change",
            {"flag": flag_name, "variant": variant, "previous": previous},
        )

    def log_load_change(self, action: str, **kwargs: Any) -> ExperimentEvent | None:
        return self.log_event("load_change", {"action": action, **kwargs})

    def log_note(self, message: str) -> ExperimentEvent | None:
        return self.log_event("note", {"message": message})

    def export_json(self, path: Path | str) -> None:
        if not self._experiment:
            return
        Path(path).write_text(json.dumps(self._experiment.to_dict(), indent=2) + "\n")

    @staticmethod
    def load_flag_snapshot(path: Path | str) -> dict[str, str]:
        """Load flag states from an exported experiment JSON.

        Replays all flag_change events to determine the final state of each flag.
        Returns a dict of {flag_name: variant}.
        """
        data = json.loads(Path(path).read_text())
        snapshot: dict[str, str] = {}
        for event in data.get("events", []):
            if event.get("event_type") == "flag_change":
                details = event.get("details", {})
                flag = details.get("flag")
                variant = details.get("variant")
                if flag and variant:
                    snapshot[flag] = variant
        return snapshot

    def export_csv(self, path: Path | str) -> None:
        if not self._experiment:
            return
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "event_type", "details"])
            for event in self._experiment.events:
                writer.writerow([
                    event.timestamp.isoformat(),
                    event.event_type,
                    json.dumps(event.details),
                ])
