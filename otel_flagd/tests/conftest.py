"""Shared test fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from otelfl.core.flagd_client import FlagdClient
from otelfl.core.experiment_logger import ExperimentLogger


@pytest.fixture
def sample_config() -> dict:
    """A minimal flagd config for testing."""
    return {
        "$schema": "https://flagd.dev/schema/v0/flags.json",
        "flags": {
            "boolFlag": {
                "description": "A boolean flag",
                "state": "ENABLED",
                "variants": {"on": True, "off": False},
                "defaultVariant": "off",
            },
            "multiFlag": {
                "description": "A multi-variant flag",
                "state": "ENABLED",
                "variants": {"off": 0, "low": 25, "high": 75, "full": 100},
                "defaultVariant": "off",
            },
        },
    }


class FlagdMockTransport(httpx.BaseTransport):
    """In-memory mock for the flagd-ui HTTP API."""

    def __init__(self, initial_config: dict) -> None:
        self._config = initial_config

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET" and path.endswith("/api/read"):
            return httpx.Response(200, json=self._config)
        elif request.method == "POST" and path.endswith("/api/write"):
            body = json.loads(request.content)
            self._config = body["data"]
            return httpx.Response(200, json={})
        return httpx.Response(404)


@pytest.fixture
def flagd_client(sample_config: dict) -> FlagdClient:
    transport = FlagdMockTransport(sample_config)
    client = FlagdClient("http://test-flagd:8080/feature")
    client._http = httpx.Client(transport=transport)
    return client


@pytest.fixture
def real_config() -> dict:
    """Full flagd config matching the OpenTelemetry demo."""
    config_path = Path.home() / "sideProjects/opentelemetry-demo/src/flagd/demo.flagd.json"
    if not config_path.exists():
        pytest.skip("Real flagd config not found")
    return json.loads(config_path.read_text())


@pytest.fixture
def real_flagd_client(real_config: dict) -> FlagdClient:
    """FlagdClient backed by the full OTel demo flag set, mocked over HTTP."""
    transport = FlagdMockTransport(real_config)
    client = FlagdClient("http://test-flagd:8080/feature")
    client._http = httpx.Client(transport=transport)
    return client


@pytest.fixture
def experiment_logger() -> ExperimentLogger:
    return ExperimentLogger()
