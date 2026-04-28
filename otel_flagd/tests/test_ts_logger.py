"""Tests for ts_logger module."""

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from otelfl.core.ts_logger import build_event, append_event


class TestBuildEventLoad:
    def test_normal_mode(self):
        args = Namespace(command="load", load_action="start", mode="normal",
                         users=None, rate=None, run_time=None)
        event = build_event(args)
        assert event is not None
        assert event["action"] == "load_start"
        assert event["mode"] == "normal"
        assert event["users"] == 5
        assert event["spawn_rate"] == 1.0
        assert event["anomaly"] is False
        assert "ts" in event

    def test_high_mode(self):
        args = Namespace(command="load", load_action="start", mode="high",
                         users=None, rate=None, run_time="5m")
        event = build_event(args)
        assert event["anomaly"] is True
        assert event["mode"] == "high"
        assert event["users"] == 20
        assert event["spawn_rate"] == 2.0
        assert event["run_time"] == "5m"

    def test_low_mode(self):
        args = Namespace(command="load", load_action="start", mode="low",
                         users=None, rate=None, run_time=None)
        event = build_event(args)
        assert event["anomaly"] is False
        assert event["mode"] == "low"
        assert event["users"] == 2

    def test_custom_users(self):
        args = Namespace(command="load", load_action="start", mode=None,
                         users=40, rate=2.0, run_time="5m")
        event = build_event(args)
        assert event["anomaly"] is True
        assert event["users"] == 40
        assert event["spawn_rate"] == 2.0
        assert "mode" not in event

    def test_stop(self):
        args = Namespace(command="load", load_action="stop")
        event = build_event(args)
        assert event["action"] == "load_stop"
        assert event["anomaly"] is False

    def test_status_returns_none(self):
        args = Namespace(command="load", load_action="status")
        assert build_event(args) is None

    def test_reset_stats_returns_none(self):
        args = Namespace(command="load", load_action="reset-stats")
        assert build_event(args) is None


class TestBuildEventFlag:
    def test_enable(self):
        args = Namespace(command="flag", flag_action="enable", name="adServiceFailure")
        event = build_event(args)
        assert event["action"] == "flag_enable"
        assert event["flag"] == "adServiceFailure"
        assert event["anomaly"] is True

    def test_disable(self):
        args = Namespace(command="flag", flag_action="disable", name="adServiceFailure")
        event = build_event(args)
        assert event["action"] == "flag_disable"
        assert event["anomaly"] is False

    def test_set_non_off(self):
        args = Namespace(command="flag", flag_action="set", name="loadgeneratorFloodHomepage",
                         variant="on")
        event = build_event(args)
        assert event["action"] == "flag_set"
        assert event["variant"] == "on"
        assert event["anomaly"] is True

    def test_set_off(self):
        args = Namespace(command="flag", flag_action="set", name="loadgeneratorFloodHomepage",
                         variant="off")
        event = build_event(args)
        assert event["anomaly"] is False

    def test_toggle(self):
        args = Namespace(command="flag", flag_action="toggle", name="adServiceFailure")
        event = build_event(args)
        assert event["action"] == "flag_toggle"
        assert event["anomaly"] is True

    def test_reset(self):
        args = Namespace(command="flag", flag_action="reset", name="all")
        event = build_event(args)
        assert event["action"] == "flag_reset"
        assert event["anomaly"] is False

    def test_restore(self):
        args = Namespace(command="flag", flag_action="restore", path="/tmp/snap.json")
        event = build_event(args)
        assert event["action"] == "flag_restore"
        assert event["anomaly"] is False

    def test_snapshot(self):
        args = Namespace(command="flag", flag_action="snapshot", path="/tmp/snap.json")
        event = build_event(args)
        assert event["action"] == "flag_snapshot"
        assert event["anomaly"] is False

    def test_list_returns_none(self):
        args = Namespace(command="flag", flag_action="list")
        assert build_event(args) is None

    def test_get_returns_none(self):
        args = Namespace(command="flag", flag_action="get", name="adServiceFailure")
        assert build_event(args) is None


class TestBuildEventScenario:
    def test_apply(self):
        args = Namespace(command="scenario", scenario_action="apply", name="total_failure")
        event = build_event(args)
        assert event["action"] == "scenario_apply"
        assert event["scenario"] == "total_failure"
        assert event["anomaly"] is True

    def test_list_returns_none(self):
        args = Namespace(command="scenario", scenario_action="list")
        assert build_event(args) is None


class TestAppendEvent:
    def test_creates_new_file(self, tmp_path: Path):
        event = {"action": "load_start", "anomaly": False, "ts": "2026-01-01T00:00:00.000Z"}
        append_event("test_run", event, ts_dir=tmp_path)
        data = json.loads((tmp_path / "test_run.json").read_text())
        assert data["scenario"] == "test_run"
        assert len(data["events"]) == 1
        assert data["events"][0]["action"] == "load_start"

    def test_appends_to_existing(self, tmp_path: Path):
        existing = {"scenario": "test_run", "events": [
            {"action": "load_start", "ts": "2026-01-01T00:00:00.000Z"}
        ]}
        (tmp_path / "test_run.json").write_text(json.dumps(existing))

        event = {"action": "load_stop", "ts": "2026-01-01T00:01:00.000Z"}
        append_event("test_run", event, ts_dir=tmp_path)

        data = json.loads((tmp_path / "test_run.json").read_text())
        assert len(data["events"]) == 2
        assert data["events"][1]["action"] == "load_stop"

    def test_uses_cwd_when_no_dir(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        event = {"action": "load_stop", "ts": "2026-01-01T00:00:00.000Z"}
        append_event("cwd_test", event)
        assert (tmp_path / "cwd_test.json").exists()
