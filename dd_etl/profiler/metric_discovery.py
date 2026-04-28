"""Discover metrics from Datadog API and build MetricFamily dicts.

Produces the same MetricFamily type that otel_etl's schema_generator expects.
"""

from __future__ import annotations

import re
import logging

from otel_etl.profiler.metric_discovery import MetricFamily, filter_otel_metrics
from otel_etl.utils.name_sanitizer import classify_metric_type

from dd_etl.utils.tag_mapper import normalize_dd_metric_name
from dd_etl.config.defaults import DD_TYPE_MAPPING, DD_HISTOGRAM_SUFFIXES

logger = logging.getLogger(__name__)


def discover_metrics(
    client,  # DatadogClient
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
) -> dict[str, MetricFamily]:
    """Discover metrics from Datadog and group them into families.

    DD histogram sub-metrics (e.g. metric.avg, metric.95percentile) are
    grouped under the base metric family.

    DD metric types are mapped:
      - "gauge"        → MetricFamily(type="gauge")
      - "count"        → MetricFamily(type="counter")
      - "rate"         → MetricFamily(type="gauge")
      - "distribution" → MetricFamily(type="gauge")

    Args:
        client: DatadogClient instance.
        include_patterns: Regex patterns to include.
        exclude_patterns: Regex patterns to exclude.

    Returns:
        dict of family_name → MetricFamily.
    """
    raw_names = client.get_metric_names()
    logger.info(f"Discovered {len(raw_names)} raw DD metrics")

    # Fetch metadata for type detection (batch, with caching)
    type_cache: dict[str, str] = {}
    for name in raw_names:
        try:
            meta = client.get_metric_metadata(name)
            type_cache[name] = meta.get("type", "gauge")
        except Exception:
            type_cache[name] = "gauge"

    families: dict[str, MetricFamily] = {}

    for raw_name in raw_names:
        dd_type = type_cache.get(raw_name, "gauge")

        # Detect histogram sub-metrics and group under base family
        base_name = raw_name
        is_hist_sub = False
        for suffix in DD_HISTOGRAM_SUFFIXES:
            if raw_name.endswith(suffix):
                base_name = raw_name[: -len(suffix)]
                is_hist_sub = True
                break

        family_name = normalize_dd_metric_name(base_name)

        # Determine normalized metric name (with _total for counts)
        type_info = DD_TYPE_MAPPING.get(dd_type, (None, "gauge"))
        name_suffix, otel_type = type_info

        if is_hist_sub:
            # Histogram sub-metrics are gauges
            normalized_metric = normalize_dd_metric_name(raw_name)
            family_type = "gauge"
        else:
            normalized_metric = family_name
            if name_suffix and not normalized_metric.endswith(name_suffix):
                normalized_metric = normalized_metric + name_suffix

            if otel_type == "counter":
                family_type = "counter"
            else:
                family_type = "gauge"

        if family_name not in families:
            families[family_name] = MetricFamily(
                name=family_name,
                type=family_type,
                metrics=[],
            )

        families[family_name]["metrics"].append(normalized_metric)

    logger.info(f"Grouped into {len(families)} metric families")

    # Apply include/exclude filters
    if include_patterns or exclude_patterns:
        families = filter_otel_metrics(families, include_patterns, exclude_patterns)
        logger.info(f"Filtered to {len(families)} families")

    return families
