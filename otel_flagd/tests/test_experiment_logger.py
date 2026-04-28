"""Tests for ExperimentLogger."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from otelfl.core.experiment_logger import ExperimentLogger


class TestExperimentLifecycle:
    def test_start_creates_experiment(self, experiment_logger: ExperimentLogger) -> None:
        exp = experiment_logger.start("test-exp")
        assert exp.name == "test-exp"
        assert exp.started_at is not None
        assert exp.stopped_at is None
        assert experiment_logger.active is True

    def test_stop_sets_stopped_at(self, experiment_logger: ExperimentLogger) -> None:
        experiment_logger.start("test")
        exp = experiment_logger.stop()
        assert exp.stopped_at is not None
        assert experiment_logger.active is False

    def test_stop_without_start(self, experiment_logger: ExperimentLogger) -> None:
        result = experiment_logger.stop()
        assert result is None

    def test_double_stop(self, experiment_logger: ExperimentLogger) -> None:
        experiment_logger.start("test")
        experiment_logger.stop()
        exp = experiment_logger.stop()
        # Second stop is a no-op (already stopped)
        assert exp.stopped_at is not None


class TestEventLogging:
    def test_log_event_when_active(self, experiment_logger: ExperimentLogger) -> None:
        experiment_logger.start("test")
        event = experiment_logger.log_event("note", {"message": "hello"})
        assert event is not None
        assert event.event_type == "note"
        assert len(experiment_logger.experiment.events) == 1

    def test_log_event_noop_when_inactive(self, experiment_logger: ExperimentLogger) -> None:
        result = experiment_logger.log_event("note", {"message": "hello"})
        assert result is None

    def test_log_event_noop_after_stop(self, experiment_logger: ExperimentLogger) -> None:
        experiment_logger.start("test")
        experiment_logger.stop()
        result = experiment_logger.log_event("note", {"message": "hello"})
        assert result is None

    def test_log_flag_change(self, experiment_logger: ExperimentLogger) -> None:
        experiment_logger.start("test")
        event = experiment_logger.log_flag_change("myFlag", "on", "off")
        assert event.event_type == "flag_change"
        assert event.details["flag"] == "myFlag"
        assert event.details["variant"] == "on"
        assert event.details["previous"] == "off"

    def test_log_load_change(self, experiment_logger: ExperimentLogger) -> None:
        experiment_logger.start("test")
        event = experiment_logger.log_load_change("start", users=10, rate=2.0)
        assert event.event_type == "load_change"
        assert event.details["action"] == "start"
        assert event.details["users"] == 10

    def test_log_note(self, experiment_logger: ExperimentLogger) -> None:
        experiment_logger.start("test")
        event = experiment_logger.log_note("something happened")
        assert event.event_type == "note"
        assert event.details["message"] == "something happened"


class TestExport:
    def test_export_json(self, experiment_logger: ExperimentLogger, tmp_path: Path) -> None:
        experiment_logger.start("json-test")
        experiment_logger.log_note("event 1")
        experiment_logger.log_flag_change("flag1", "on", "off")
        experiment_logger.stop()

        path = tmp_path / "export.json"
        experiment_logger.export_json(path)

        data = json.loads(path.read_text())
        assert data["name"] == "json-test"
        assert data["started_at"] is not None
        assert data["stopped_at"] is not None
        assert len(data["events"]) == 2

    def test_export_csv(self, experiment_logger: ExperimentLogger, tmp_path: Path) -> None:
        experiment_logger.start("csv-test")
        experiment_logger.log_note("first")
        experiment_logger.log_load_change("start", users=5)
        experiment_logger.stop()

        path = tmp_path / "export.csv"
        experiment_logger.export_csv(path)

        with open(path) as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert rows[0] == ["timestamp", "event_type", "details"]
        assert len(rows) == 3  # header + 2 events
        assert rows[1][1] == "note"
        assert rows[2][1] == "load_change"

    def test_export_noop_without_experiment(
        self, experiment_logger: ExperimentLogger, tmp_path: Path
    ) -> None:
        path = tmp_path / "empty.json"
        experiment_logger.export_json(path)
        assert not path.exists()


class TestImport:
    def test_load_flag_snapshot_from_export(
        self, experiment_logger: ExperimentLogger, tmp_path: Path
    ) -> None:
        experiment_logger.start("import-test")
        experiment_logger.log_flag_change("flag1", "on", "off")
        experiment_logger.log_flag_change("flag2", "high", "off")
        experiment_logger.log_note("some note")
        experiment_logger.log_flag_change("flag1", "off", "on")  # flag1 changed back
        experiment_logger.stop()

        path = tmp_path / "export.json"
        experiment_logger.export_json(path)

        snapshot = ExperimentLogger.load_flag_snapshot(path)
        # flag1 was changed to "on" then back to "off"
        assert snapshot["flag1"] == "off"
        assert snapshot["flag2"] == "high"
        assert len(snapshot) == 2  # only flag_change events

    def test_load_empty_snapshot(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.json"
        path.write_text(json.dumps({"name": "empty", "events": []}))
        snapshot = ExperimentLogger.load_flag_snapshot(path)
        assert snapshot == {}
