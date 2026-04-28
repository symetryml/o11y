"""Parse Datadog Agent intake payloads into normalized metric rows.

The DD agent sends JSON payloads to intake endpoints.  This module
converts them to the standard otel_etl row format:

    {"timestamp": datetime, "metric": str, "labels": dict, "value": float, "dd_type": str}

The extra "dd_type" field is used by MetricStore for type-aware re-aggregation
but is stripped before data reaches denormalize_metrics().
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from dd_etl.utils.tag_mapper import tags_and_name_to_otel
from dd_etl.receiver.proto.metrics_pb2 import MetricPayload

logger = logging.getLogger(__name__)

V2_TYPE_MAP = {0: "gauge", 1: "count", 2: "rate", 3: "gauge"}


def parse_v1_series(payload: dict[str, Any]) -> list[dict]:
    """Parse a DD v1 ``/api/v1/series`` JSON payload.

    Expected format::

        {
          "series": [
            {
              "metric": "system.cpu.user",
              "points": [[<epoch_seconds>, <value>], ...],
              "tags": ["service:frontend", "env:prod"],
              "type": "gauge",          # gauge | count | rate
              "host": "web-01",
              "interval": 10            # optional
            },
            ...
          ]
        }

    Returns:
        List of normalised row dicts.
    """
    rows: list[dict] = []
    series_list = payload.get("series", [])

    for series in series_list:
        metric_name = series.get("metric", "")
        dd_type = series.get("type", "gauge")
        tags = series.get("tags")
        host = series.get("host")
        points = series.get("points", [])

        if not metric_name or not points:
            continue

        normalized_name, labels, otel_type = tags_and_name_to_otel(
            metric_name, dd_type, tags, host
        )

        for point in points:
            # v1 points: [epoch_seconds, value] or (epoch_seconds, value)
            if len(point) < 2:
                continue

            ts_epoch = point[0]
            value = point[1]

            if value is None:
                continue

            ts = datetime.fromtimestamp(float(ts_epoch), tz=timezone.utc)

            rows.append({
                "timestamp": ts,
                "metric": normalized_name,
                "labels": labels,
                "value": float(value),
                "dd_type": dd_type,
            })

    return rows


def parse_v2_series(payload: dict[str, Any]) -> list[dict]:
    """Parse a DD v2 ``/api/v2/series`` JSON payload.

    V2 format wraps series inside ``{"series": [...]}`` with a slightly
    different point structure.  Each series item may have::

        {
          "metric": "system.cpu.user",
          "type": 1,                      # 0=unspecified, 1=count, 2=rate, 3=gauge
          "points": [{"timestamp": <epoch>, "value": <float>}, ...],
          "tags": ["service:frontend"],
          "resources": [{"type": "host", "name": "web-01"}],
          "source_type_name": "...",
          "interval": 10
        }

    Returns:
        List of normalised row dicts (same schema as parse_v1_series).
    """
    rows: list[dict] = []
    series_list = payload.get("series", [])

    for series in series_list:
        metric_name = series.get("metric", "")
        raw_type = series.get("type", 3)
        dd_type = V2_TYPE_MAP.get(raw_type, "gauge") if isinstance(raw_type, int) else str(raw_type)
        tags = series.get("tags")
        points = series.get("points", [])

        # Extract host from resources if available
        host = None
        for resource in series.get("resources", []):
            if resource.get("type") == "host":
                host = resource.get("name")
                break

        if not metric_name or not points:
            continue

        normalized_name, labels, otel_type = tags_and_name_to_otel(
            metric_name, dd_type, tags, host
        )

        for point in points:
            if isinstance(point, dict):
                ts_epoch = point.get("timestamp")
                value = point.get("value")
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                ts_epoch, value = point[0], point[1]
            else:
                continue

            if ts_epoch is None or value is None:
                continue

            ts = datetime.fromtimestamp(float(ts_epoch), tz=timezone.utc)

            rows.append({
                "timestamp": ts,
                "metric": normalized_name,
                "labels": labels,
                "value": float(value),
                "dd_type": dd_type,
            })

    return rows


def parse_v2_protobuf(raw: bytes) -> list[dict]:
    """Parse a DD v2 protobuf-encoded MetricPayload.

    The OTel Collector Datadog exporter sends metrics as protobuf.
    """
    payload = MetricPayload()
    payload.ParseFromString(raw)

    rows: list[dict] = []

    for series in payload.series:
        metric_name = series.metric
        dd_type = V2_TYPE_MAP.get(series.type, "gauge")
        tags = list(series.tags)

        host = None
        for resource in series.resources:
            if resource.type == "host":
                host = resource.name
                break

        if not metric_name or not series.points:
            continue

        normalized_name, labels, otel_type = tags_and_name_to_otel(
            metric_name, dd_type, tags, host
        )

        for point in series.points:
            ts = datetime.fromtimestamp(float(point.timestamp), tz=timezone.utc)
            rows.append({
                "timestamp": ts,
                "metric": normalized_name,
                "labels": labels,
                "value": float(point.value),
                "dd_type": dd_type,
            })

    return rows


def parse_intake(payload: dict[str, Any]) -> list[dict]:
    """Auto-detect payload format and dispatch to the right parser."""
    series = payload.get("series", [])
    if not series:
        return []

    # Peek at first item to detect v1 vs v2
    first = series[0]
    if isinstance(first.get("type"), int):
        return parse_v2_series(payload)
    return parse_v1_series(payload)
