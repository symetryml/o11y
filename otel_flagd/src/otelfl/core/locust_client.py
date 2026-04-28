"""Sync and async HTTP clients for the Locust load generator API."""

from __future__ import annotations

from typing import Any

import httpx

from otelfl.models import EndpointStats, LocustStats


class LocustConnectionError(Exception):
    """Cannot reach the Locust API."""


class LocustAPIError(Exception):
    """Locust API returned an error."""


def _parse_stats(data: dict[str, Any]) -> LocustStats:
    """Parse Locust /stats/requests response into LocustStats."""
    agg = {}
    endpoints = []
    for entry in data.get("stats", []):
        if entry.get("name") == "Aggregated":
            agg = entry
        else:
            percentiles = entry.get("response_times", {})
            endpoints.append(EndpointStats(
                name=entry.get("name", ""),
                method=entry.get("method", ""),
                num_requests=entry.get("num_requests", 0),
                num_failures=entry.get("num_failures", 0),
                current_rps=entry.get("current_rps", 0.0),
                avg_response_time=entry.get("avg_response_time", 0.0),
                max_response_time=entry.get("max_response_time", 0.0),
                min_response_time=entry.get("min_response_time", 0.0),
                p50=percentiles.get("0.5", 0.0) if isinstance(percentiles, dict) else 0.0,
                p90=percentiles.get("0.9", 0.0) if isinstance(percentiles, dict) else 0.0,
                p99=percentiles.get("0.99", 0.0) if isinstance(percentiles, dict) else 0.0,
            ))
    # Sort by RPS descending
    endpoints.sort(key=lambda e: e.current_rps, reverse=True)
    return LocustStats(
        state=data.get("state", "unknown"),
        user_count=data.get("user_count", 0),
        total_rps=agg.get("current_rps", 0.0),
        fail_ratio=data.get("fail_ratio", 0.0),
        total_avg_response_time=agg.get("avg_response_time", 0.0),
        total_max_response_time=agg.get("max_response_time", 0.0),
        total_min_response_time=agg.get("min_response_time", 0.0),
        errors=[{"method": e.get("method"), "name": e.get("name"), "occurrences": e.get("occurrences")} for e in data.get("errors", [])],
        endpoints=endpoints,
    )


def _normalize_base_url(url: str) -> str:
    """Ensure base URL ends with / for proper httpx path resolution."""
    return url if url.endswith("/") else url + "/"


class LocustClient:
    """Synchronous Locust API client."""

    def __init__(self, base_url: str = "http://localhost:8080/loadgen/", timeout: float = 10.0) -> None:
        self.base_url = _normalize_base_url(base_url)
        self._client = httpx.Client(base_url=self.base_url, timeout=timeout)

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        # Use relative paths so httpx resolves them against base_url
        path = path.lstrip("/")
        try:
            resp = self._client.request(method, path, **kwargs)
            resp.raise_for_status()
            if resp.headers.get("content-type", "").startswith("application/json"):
                return resp.json()
            return resp.text
        except httpx.ConnectError as e:
            raise LocustConnectionError(f"Cannot connect to Locust at {self.base_url}: {e}") from e
        except httpx.HTTPStatusError as e:
            raise LocustAPIError(f"Locust API error: {e.response.status_code}") from e

    def start(
        self, users: int = 10, spawn_rate: float = 1.0, run_time: str | None = None
    ) -> dict[str, Any]:
        data: dict[str, Any] = {"user_count": users, "spawn_rate": spawn_rate}
        if run_time:
            data["run_time"] = run_time
        return self._request("POST", "swarm", data=data)

    def stop(self) -> Any:
        return self._request("GET", "stop")

    def get_stats(self) -> LocustStats:
        data = self._request("GET", "stats/requests")
        return _parse_stats(data)

    def reset_stats(self) -> Any:
        return self._request("GET", "stats/reset")

    def get_exceptions(self) -> Any:
        return self._request("GET", "exceptions")

    def close(self) -> None:
        self._client.close()


class AsyncLocustClient:
    """Asynchronous Locust API client."""

    def __init__(self, base_url: str = "http://localhost:8080/loadgen/", timeout: float = 10.0) -> None:
        self.base_url = _normalize_base_url(base_url)
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        path = path.lstrip("/")
        try:
            resp = await self._client.request(method, path, **kwargs)
            resp.raise_for_status()
            if resp.headers.get("content-type", "").startswith("application/json"):
                return resp.json()
            return resp.text
        except httpx.ConnectError as e:
            raise LocustConnectionError(f"Cannot connect to Locust at {self.base_url}: {e}") from e
        except httpx.HTTPStatusError as e:
            raise LocustAPIError(f"Locust API error: {e.response.status_code}") from e

    async def start(
        self, users: int = 10, spawn_rate: float = 1.0, run_time: str | None = None
    ) -> dict[str, Any]:
        data: dict[str, Any] = {"user_count": users, "spawn_rate": spawn_rate}
        if run_time:
            data["run_time"] = run_time
        return await self._request("POST", "swarm", data=data)

    async def stop(self) -> Any:
        return await self._request("GET", "stop")

    async def get_stats(self) -> LocustStats:
        data = await self._request("GET", "stats/requests")
        return _parse_stats(data)

    async def reset_stats(self) -> Any:
        return await self._request("GET", "stats/reset")

    async def get_exceptions(self) -> Any:
        return await self._request("GET", "exceptions")

    async def close(self) -> None:
        await self._client.aclose()
