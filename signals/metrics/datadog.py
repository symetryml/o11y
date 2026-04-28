"""Wrapper around the datadog-api-client Python SDK.

Pure data-fetching layer — returns raw Datadog data.
For DD→OTel normalization, use dd_etl.utils.tag_mapper on top of this.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class DatadogClient:
    """Thin wrapper around the datadog-api-client SDK for metrics operations.

    Reads DD_API_KEY and DD_APP_KEY from environment if not provided.
    """

    def __init__(
        self,
        api_key: str | None = None,
        app_key: str | None = None,
        site: str = "datadoghq.com",
    ):
        self.api_key = api_key or os.environ.get("DD_API_KEY", "")
        self.app_key = app_key or os.environ.get("DD_APP_KEY", "")
        self.site = site

        if not self.api_key:
            raise ValueError(
                "Datadog API key required. Pass api_key= or set DD_API_KEY env var."
            )
        if not self.app_key:
            raise ValueError(
                "Datadog APP key required. Pass app_key= or set DD_APP_KEY env var."
            )

        self._configuration = self._build_config()

    def _build_config(self):
        from datadog_api_client import Configuration

        config = Configuration()
        config.api_key["apiKeyAuth"] = self.api_key
        config.api_key["appKeyAuth"] = self.app_key
        config.server_variables["site"] = self.site
        return config

    # ------------------------------------------------------------------
    # Metric discovery
    # ------------------------------------------------------------------

    def get_metric_names(self, window_hours: float = 1.0) -> list[str]:
        """List active metric names reported in the last *window_hours*.

        Uses v1 ``list_active_metrics``.
        """
        from datadog_api_client import ApiClient
        from datadog_api_client.v1.api.metrics_api import MetricsApi

        _from = int(time.time()) - int(window_hours * 3600)

        with ApiClient(self._configuration) as api_client:
            api = MetricsApi(api_client)
            response = api.list_active_metrics(_from=_from)
            return sorted(response.metrics or [])

    def get_tags_for_metric(
        self, metric_name: str, window_seconds: int = 14400
    ) -> list[str]:
        """Get all tags (keys only) seen on a metric.

        Uses v2 ``list_tags_by_metric_name``.
        """
        from datadog_api_client import ApiClient
        from datadog_api_client.v2.api.metrics_api import MetricsApi as MetricsApiV2

        with ApiClient(self._configuration) as api_client:
            api = MetricsApiV2(api_client)
            response = api.list_tags_by_metric_name(metric_name=metric_name)
            return sorted(response.data.attributes.tags or [])

    def get_metric_metadata(self, metric_name: str) -> dict[str, Any]:
        """Get metric metadata (type, unit, description).

        Uses v1 ``get_metric_metadata``.
        """
        from datadog_api_client import ApiClient
        from datadog_api_client.v1.api.metrics_api import MetricsApi

        with ApiClient(self._configuration) as api_client:
            api = MetricsApi(api_client)
            meta = api.get_metric_metadata(metric_name=metric_name)
            return {
                "type": getattr(meta, "type", "gauge"),
                "unit": getattr(meta, "unit", None),
                "per_unit": getattr(meta, "per_unit", None),
                "description": getattr(meta, "description", ""),
                "integration": getattr(meta, "integration", ""),
            }

    # ------------------------------------------------------------------
    # Tag value discovery (for cardinality analysis)
    # ------------------------------------------------------------------

    def get_tag_values(
        self,
        metric_name: str,
        tag_name: str,
        window_hours: float = 1.0,
    ) -> list[str]:
        """Get distinct values for a tag by querying with ``by {tag}``.

        This executes a real query against the DD API and extracts unique
        tag values from the returned series.
        """
        from datadog_api_client import ApiClient
        from datadog_api_client.v1.api.metrics_api import MetricsApi

        now = int(time.time())
        _from = now - int(window_hours * 3600)
        query = f"avg:{metric_name}{{*}} by {{{tag_name}}}"

        with ApiClient(self._configuration) as api_client:
            api = MetricsApi(api_client)
            response = api.query_metrics(_from=_from, to=now, query=query)

        values = set()
        for series in (response.series or []):
            for tag_str in (series.tag_set or []):
                if ":" in tag_str:
                    k, _, v = tag_str.partition(":")
                    if k == tag_name:
                        values.add(v)
        return sorted(values)

    def count_tag_cardinality(
        self,
        metric_name: str,
        tag_name: str,
        window_hours: float = 1.0,
    ) -> int:
        """Count distinct values for a tag on a metric."""
        return len(self.get_tag_values(metric_name, tag_name, window_hours))

    def get_top_n_values(
        self,
        metric_name: str,
        tag_name: str,
        n: int = 20,
        window_hours: float = 1.0,
    ) -> list[str]:
        """Return top-N tag values (by occurrence in query results)."""
        values = self.get_tag_values(metric_name, tag_name, window_hours)
        return values[:n]

    # ------------------------------------------------------------------
    # Timeseries query
    # ------------------------------------------------------------------

    def query_metrics(
        self,
        query: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """Execute a DD query and return a DataFrame with raw DD data.

        Args:
            query: Datadog query string (e.g. "avg:system.cpu.user{*} by {host}").
            start: Start of range.
            end: End of range.

        Returns:
            DataFrame(timestamp, metric, tags, value) where metric and tags
            are in raw Datadog format (dot-separated names, "key:value" tag dicts).
        """
        from datadog_api_client import ApiClient
        from datadog_api_client.v1.api.metrics_api import MetricsApi

        _from = int(start.timestamp())
        _to = int(end.timestamp())

        with ApiClient(self._configuration) as api_client:
            api = MetricsApi(api_client)
            response = api.query_metrics(_from=_from, to=_to, query=query)

        rows: list[dict] = []
        for series in (response.series or []):
            raw_metric = series.metric or ""

            # Parse "key:value" tag strings into a dict
            tags_dict: dict[str, str] = {}
            for tag_str in (series.tag_set or []):
                if ":" in tag_str:
                    k, _, v = tag_str.partition(":")
                    tags_dict[k.strip()] = v.strip()
                else:
                    tags_dict[tag_str.strip()] = "true"

            for point in (series.pointlist or []):
                ts_ms = point[0]
                value = point[1]
                if value is None:
                    continue
                ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                rows.append({
                    "timestamp": ts,
                    "metric": raw_metric,
                    "tags": tags_dict,
                    "value": float(value),
                })

        return pd.DataFrame(rows) if rows else pd.DataFrame(
            columns=["timestamp", "metric", "tags", "value"]
        )

    def query_metrics_range(
        self,
        metric_names: list[str],
        start: datetime,
        end: datetime,
        step: str = "60s",
        aggregation: str = "avg",
        group_by: list[str] | None = None,
    ) -> pd.DataFrame:
        """Fetch multiple metrics and return a combined DataFrame.

        Args:
            metric_names: List of DD metric names (dot-separated).
            start: Start of range.
            end: End of range.
            step: Rollup interval (e.g. "60s").
            aggregation: Space aggregation ("avg", "sum", "min", "max").
            group_by: Tags to split by (default: all).

        Returns:
            Combined DataFrame(timestamp, metric, tags, value).
        """
        frames = []
        by_clause = f" by {{{','.join(group_by)}}}" if group_by else ""

        for metric_name in metric_names:
            query = f"{aggregation}:{metric_name}{{*}}{by_clause}.rollup({aggregation}, {_parse_step(step)})"
            try:
                df = self.query_metrics(query, start, end)
                if not df.empty:
                    frames.append(df)
            except Exception as e:
                logger.warning(f"Failed to query {metric_name}: {e}")

        if not frames:
            return pd.DataFrame(columns=["timestamp", "metric", "tags", "value"])

        return pd.concat(frames, ignore_index=True)


def _parse_step(step: str) -> int:
    """Convert step string to seconds."""
    step = step.strip().lower()
    if step.endswith("s"):
        return int(step[:-1])
    if step.endswith("m"):
        return int(step[:-1]) * 60
    if step.endswith("h"):
        return int(step[:-1]) * 3600
    return int(step)
