"""Tests for CLI load commands --mode option."""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock

import pytest
from rich.console import Console

from otelfl.cli import load_commands
from otelfl.models import RUN_MODES


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    client.start.return_value = {"success": True, "message": "Swarming started"}
    return client


@pytest.fixture
def console() -> Console:
    return Console(no_color=True)


def _make_args(**kwargs) -> argparse.Namespace:
    defaults = {
        "load_action": "start",
        "mode": None,
        "users": None,
        "rate": None,
        "run_time": None,
        "output_format": "text",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestModeOption:
    def test_mode_low(self, mock_client: MagicMock, console: Console) -> None:
        args = _make_args(mode="low")
        load_commands.run(args, mock_client, console)
        mock_client.start.assert_called_once_with(users=2, spawn_rate=1.0, run_time=None)

    def test_mode_normal(self, mock_client: MagicMock, console: Console) -> None:
        args = _make_args(mode="normal")
        load_commands.run(args, mock_client, console)
        mock_client.start.assert_called_once_with(users=5, spawn_rate=1.0, run_time=None)

    def test_mode_high(self, mock_client: MagicMock, console: Console) -> None:
        args = _make_args(mode="high")
        load_commands.run(args, mock_client, console)
        mock_client.start.assert_called_once_with(users=20, spawn_rate=2.0, run_time=None)

    def test_mode_with_user_override(self, mock_client: MagicMock, console: Console) -> None:
        args = _make_args(mode="high", users=50)
        load_commands.run(args, mock_client, console)
        mock_client.start.assert_called_once_with(users=50, spawn_rate=2.0, run_time=None)

    def test_mode_with_rate_override(self, mock_client: MagicMock, console: Console) -> None:
        args = _make_args(mode="low", rate=5.0)
        load_commands.run(args, mock_client, console)
        mock_client.start.assert_called_once_with(users=2, spawn_rate=5.0, run_time=None)

    def test_mode_with_run_time(self, mock_client: MagicMock, console: Console) -> None:
        args = _make_args(mode="normal", run_time="5m")
        load_commands.run(args, mock_client, console)
        mock_client.start.assert_called_once_with(users=5, spawn_rate=1.0, run_time="5m")

    def test_no_mode_defaults(self, mock_client: MagicMock, console: Console) -> None:
        args = _make_args()
        load_commands.run(args, mock_client, console)
        mock_client.start.assert_called_once_with(users=10, spawn_rate=1.0, run_time=None)

    def test_no_mode_with_manual_values(self, mock_client: MagicMock, console: Console) -> None:
        args = _make_args(users=30, rate=3.0)
        load_commands.run(args, mock_client, console)
        mock_client.start.assert_called_once_with(users=30, spawn_rate=3.0, run_time=None)
