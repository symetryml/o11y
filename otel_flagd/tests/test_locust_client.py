"""Tests for LocustClient using mocked httpx transport."""

from __future__ import annotations

import json

import httpx
import pytest

from otelfl.core.locust_client import (
    LocustClient,
    AsyncLocustClient,
    LocustConnectionError,
    LocustAPIError,
    _parse_stats,
)


SAMPLE_STATS_RESPONSE = {
    "state": "running",
    "user_count": 10,
    "fail_ratio": 0.05,
    "errors": [],
    "stats": [
        {
            "name": "/api/products",
            "method": "GET",
            "current_rps": 5.0,
            "avg_response_time": 120.0,
        },
        {
            "name": "Aggregated",
            "method": None,
            "current_rps": 15.5,
            "avg_response_time": 200.0,
            "max_response_time": 1500.0,
            "min_response_time": 10.0,
        },
    ],
}


def _make_mock_transport(responses: dict[str, tuple[int, dict | str]]) -> httpx.MockTransport:
    """Create a mock transport that returns canned responses by path."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path in responses:
            status, body = responses[path]
            if isinstance(body, dict):
                return httpx.Response(
                    status, json=body, headers={"content-type": "application/json"}
                )
            return httpx.Response(status, text=body)
        return httpx.Response(404, text="Not found")

    return httpx.MockTransport(handler)


class TestParseStats:
    def test_parse_stats_extracts_aggregated(self) -> None:
        stats = _parse_stats(SAMPLE_STATS_RESPONSE)
        assert stats.state == "running"
        assert stats.user_count == 10
        assert stats.total_rps == 15.5
        assert stats.fail_ratio == 0.05
        assert stats.total_avg_response_time == 200.0
        assert stats.total_max_response_time == 1500.0

    def test_parse_stats_no_aggregated(self) -> None:
        stats = _parse_stats({"state": "stopped", "stats": [], "errors": []})
        assert stats.state == "stopped"
        assert stats.total_rps == 0.0


class TestLocustClient:
    def test_get_stats(self) -> None:
        transport = _make_mock_transport({"/stats/requests": (200, SAMPLE_STATS_RESPONSE)})
        client = LocustClient(base_url="http://test")
        client._client = httpx.Client(transport=transport, base_url="http://test")
        stats = client.get_stats()
        assert stats.state == "running"
        assert stats.total_rps == 15.5

    def test_start(self) -> None:
        transport = _make_mock_transport({
            "/swarm": (200, {"success": True, "message": "Swarming started"}),
        })
        client = LocustClient(base_url="http://test")
        client._client = httpx.Client(transport=transport, base_url="http://test")
        result = client.start(users=20, spawn_rate=2.0)
        assert result["success"] is True

    def test_stop(self) -> None:
        transport = _make_mock_transport({
            "/stop": (200, {"success": True, "message": "Test stopped"}),
        })
        client = LocustClient(base_url="http://test")
        client._client = httpx.Client(transport=transport, base_url="http://test")
        result = client.stop()
        assert result["success"] is True

    def test_reset_stats(self) -> None:
        transport = _make_mock_transport({"/stats/reset": (200, "OK")})
        client = LocustClient(base_url="http://test")
        client._client = httpx.Client(transport=transport, base_url="http://test")
        result = client.reset_stats()
        assert result == "OK"

    def test_connection_error(self) -> None:
        def handler(request):
            raise httpx.ConnectError("Connection refused")

        client = LocustClient(base_url="http://test")
        client._client = httpx.Client(
            transport=httpx.MockTransport(handler), base_url="http://test"
        )
        with pytest.raises(LocustConnectionError):
            client.get_stats()

    def test_api_error(self) -> None:
        transport = _make_mock_transport({"/stats/requests": (500, "Internal error")})
        client = LocustClient(base_url="http://test")
        client._client = httpx.Client(transport=transport, base_url="http://test")
        with pytest.raises(LocustAPIError):
            client.get_stats()


class TestAsyncLocustClient:
    @pytest.mark.asyncio
    async def test_get_stats(self) -> None:
        transport = _make_mock_transport({"/stats/requests": (200, SAMPLE_STATS_RESPONSE)})
        client = AsyncLocustClient(base_url="http://test")
        client._client = httpx.AsyncClient(transport=transport, base_url="http://test")
        stats = await client.get_stats()
        assert stats.state == "running"
        assert stats.total_rps == 15.5
        await client.close()

    @pytest.mark.asyncio
    async def test_start(self) -> None:
        transport = _make_mock_transport({
            "/swarm": (200, {"success": True}),
        })
        client = AsyncLocustClient(base_url="http://test")
        client._client = httpx.AsyncClient(transport=transport, base_url="http://test")
        result = await client.start(users=5)
        assert result["success"] is True
        await client.close()

    @pytest.mark.asyncio
    async def test_connection_error(self) -> None:
        def handler(request):
            raise httpx.ConnectError("Connection refused")

        client = AsyncLocustClient(base_url="http://test")
        client._client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://test"
        )
        with pytest.raises(LocustConnectionError):
            await client.get_stats()
        await client.close()
