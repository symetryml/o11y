"""In-memory metric buffer with type-aware re-aggregation.

Receives normalised metric rows (10s DD agent flush) and provides
``fetch_metrics_range()`` that re-aggregates to any requested step size,
returning the standard otel_etl DataFrame contract:

    DataFrame(columns=["timestamp", "metric", "labels", "value"])

Stateless — no disk persistence. On restart, gaps are backfilled
from the Datadog API using the checkpoint timestamp.
"""

from __future__ import annotations

import json
import logging
import threading
from collections import deque
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from dd_etl.config.defaults import DEFAULT_BUFFER_RETENTION_HOURS

logger = logging.getLogger(__name__)


def _labels_to_str(labels: dict[str, str]) -> str:
    """Deterministic string key for a labels dict (for grouping)."""
    return json.dumps(labels, sort_keys=True, separators=(",", ":"))


def _str_to_labels(s: str) -> dict[str, str]:
    return json.loads(s)


def _parse_step(step: str) -> int:
    """Convert a step string like '60s', '5m', '1h' to seconds."""
    step = step.strip().lower()
    if step.endswith("s"):
        return int(step[:-1])
    if step.endswith("m"):
        return int(step[:-1]) * 60
    if step.endswith("h"):
        return int(step[:-1]) * 3600
    return int(step)


class MetricStore:
    """In-memory ring buffer for metric rows.

    Thread-safe: append() and fetch_metrics_range() acquire a lock.
    Old data beyond ``retention_hours`` is pruned on each append.
    """

    def __init__(self, retention_hours: int = DEFAULT_BUFFER_RETENTION_HOURS):
        self.retention_hours = retention_hours
        self._buffer: deque[dict] = deque()
        self._lock = threading.Lock()
        self._type_registry: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def append(self, rows: list[dict]) -> None:
        """Append normalised metric rows to the buffer."""
        with self._lock:
            for row in rows:
                self._type_registry[row["metric"]] = row.get("dd_type", "gauge")
            self._buffer.extend(rows)
            self._prune()

    def buffered_count(self) -> int:
        with self._lock:
            return len(self._buffer)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def fetch_metrics_range(
        self,
        metric_names: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        step: str = "60s",
    ) -> pd.DataFrame:
        """Query the buffer and re-aggregate to *step*.

        Args:
            metric_names: Metrics to include (None = all).
            start: Start of range (inclusive).
            end: End of range (inclusive).
            step: Re-aggregation window, e.g. "60s", "5m".

        Returns:
            DataFrame with columns: timestamp, metric, labels, value
        """
        with self._lock:
            rows = list(self._buffer)

        if not rows:
            return pd.DataFrame(columns=["timestamp", "metric", "labels", "value"])

        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Filter by time
        if start is not None:
            ts_start = pd.Timestamp(start)
            if ts_start.tzinfo is None:
                ts_start = ts_start.tz_localize("UTC")
            df = df[df["timestamp"] >= ts_start]
        if end is not None:
            ts_end = pd.Timestamp(end)
            if ts_end.tzinfo is None:
                ts_end = ts_end.tz_localize("UTC")
            df = df[df["timestamp"] <= ts_end]

        # Filter by metric names
        if metric_names is not None:
            df = df[df["metric"].isin(metric_names)]

        if df.empty:
            return pd.DataFrame(columns=["timestamp", "metric", "labels", "value"])

        step_seconds = _parse_step(step)
        df = self._reaggregate(df, step_seconds)
        df = self._counts_to_cumulative(df)

        return df[["timestamp", "metric", "labels", "value"]].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _reaggregate(self, df: pd.DataFrame, step_seconds: int) -> pd.DataFrame:
        """Re-aggregate 10s data into *step_seconds* windows.

        Type-aware:
          - gauge  -> last value in window
          - count  -> sum of deltas in window
          - rate   -> mean of rates in window
        """
        origin = df["timestamp"].min()
        df["window"] = (
            (df["timestamp"] - origin).dt.total_seconds() // step_seconds
        ).astype(int)
        df["window_ts"] = origin + pd.to_timedelta(df["window"] * step_seconds, unit="s")
        df["labels_key"] = df["labels"].apply(_labels_to_str)

        groups = df.groupby(["metric", "labels_key", "window"], sort=False)

        results: list[dict] = []
        for (metric, labels_key, _win), grp in groups:
            dd_type = self._type_registry.get(metric, "gauge")
            if "dd_type" in grp.columns and not grp["dd_type"].empty:
                dd_type = grp["dd_type"].iloc[0]

            if dd_type == "count":
                agg_value = grp["value"].sum()
            elif dd_type == "rate":
                agg_value = grp["value"].mean()
            else:
                agg_value = grp.sort_values("timestamp")["value"].iloc[-1]

            results.append({
                "timestamp": grp["window_ts"].iloc[0],
                "metric": metric,
                "labels": _str_to_labels(labels_key),
                "value": agg_value,
                "dd_type": dd_type,
            })

        return pd.DataFrame(results)

    def _counts_to_cumulative(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert count (delta) metrics to cumulative sums.

        otel_etl's counter aggregator expects cumulative counters.
        DD count metrics are deltas, so we cumsum per series.
        """
        if df.empty or "dd_type" not in df.columns:
            return df

        is_count = df["dd_type"] == "count"
        if not is_count.any():
            return df

        df = df.copy()
        count_df = df[is_count].copy()
        other_df = df[~is_count].copy()

        count_df["labels_key"] = count_df["labels"].apply(_labels_to_str)
        count_df = count_df.sort_values(["metric", "labels_key", "timestamp"])
        count_df["value"] = count_df.groupby(["metric", "labels_key"])["value"].cumsum()
        count_df = count_df.drop(columns=["labels_key"])

        return pd.concat([other_df, count_df], ignore_index=True)

    def _prune(self) -> None:
        """Remove rows older than retention window. Must hold _lock."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.retention_hours)
        while self._buffer and self._buffer[0].get("timestamp", cutoff) < cutoff:
            self._buffer.popleft()

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def get_metric_names(self) -> list[str]:
        return sorted(self._type_registry.keys())

    def get_type_registry(self) -> dict[str, str]:
        return dict(self._type_registry)
