"""Wrapper around the datadog-api-client Python SDK.

Canonical location: signals.metrics.datadog
This module re-exports DatadogClient and adds DD→OTel normalization to
query_metrics / query_metrics_range for backward compatibility.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from signals.metrics.datadog import (  # noqa: F401
    DatadogClient as _BaseDatadogClient,
    _parse_step,
)

from dd_etl.utils.tag_mapper import (
    normalize_dd_metric_name,
    parse_dd_tags,
    map_dd_tags_to_otel,
)

logger = logging.getLogger(__name__)


class DatadogClient(_BaseDatadogClient):
    """DatadogClient with DD→OTel normalization on query results.

    Discovery methods (get_metric_names, get_tags_for_metric, etc.)
    are inherited unchanged. query_metrics / query_metrics_range return
    normalized otel_etl-style DataFrames (metric names underscored,
    tags mapped to OTel label names).
    """

    def query_metrics(
        self,
        query: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """Execute a DD query and return an otel_etl-normalized DataFrame.

        Returns:
            DataFrame(timestamp, metric, labels, value) with normalized
            metric names and OTel-style label keys.
        """
        raw_df = super().query_metrics(query, start, end)
        if raw_df.empty:
            return pd.DataFrame(
                columns=["timestamp", "metric", "labels", "value"]
            )

        raw_df["metric"] = raw_df["metric"].apply(normalize_dd_metric_name)
        raw_df["labels"] = raw_df["tags"].apply(map_dd_tags_to_otel)
        return raw_df[["timestamp", "metric", "labels", "value"]]
