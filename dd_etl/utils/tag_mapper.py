"""Map Datadog tags to OTel-style labels.

Datadog tags are "key:value" strings (e.g., "service:frontend").
OTel / otel_etl expects a dict[str, str] with standardized key names
(e.g., {"service_name": "frontend"}).
"""

from __future__ import annotations

from otel_etl.utils.name_sanitizer import sanitize_name

from dd_etl.config.defaults import (
    DD_TAG_MAPPING,
    DD_TYPE_MAPPING,
    DD_HISTOGRAM_SUFFIXES,
)


def parse_dd_tags(tags: list[str] | None) -> dict[str, str]:
    """Parse Datadog tag list into a key-value dict.

    Args:
        tags: List of "key:value" strings, e.g. ["service:frontend", "env:prod"].
              Bare tags (no colon) are stored with value "true".

    Returns:
        Dict of tag_key -> tag_value.
    """
    if not tags:
        return {}

    result: dict[str, str] = {}
    for tag in tags:
        if ":" in tag:
            key, _, value = tag.partition(":")
            result[key.strip()] = value.strip()
        else:
            # Bare tag — store with a truthy sentinel
            result[tag.strip()] = "true"
    return result


def map_dd_tags_to_otel(
    tags_dict: dict[str, str],
    mapping: dict[str, str] | None = None,
) -> dict[str, str]:
    """Rename Datadog tag keys to OTel-style label names.

    Args:
        tags_dict: Parsed tag dict (from parse_dd_tags).
        mapping: Custom key mapping. Defaults to DD_TAG_MAPPING.

    Returns:
        New dict with renamed keys. Unmapped keys pass through unchanged.
    """
    m = mapping or DD_TAG_MAPPING
    return {m.get(k, k): v for k, v in tags_dict.items()}


def normalize_dd_metric_name(name: str) -> str:
    """Convert a Datadog metric name to the underscore convention used by otel_etl.

    Examples:
        "system.cpu.user"           -> "system_cpu_user"
        "http.server.duration"      -> "http_server_duration"
        "trace.http.request.hits"   -> "trace_http_request_hits"
    """
    return sanitize_name(name)


def map_dd_metric_type(
    dd_type: str,
    metric_name: str,
) -> tuple[str, str]:
    """Map a Datadog metric type to a (normalized_name, otel_type) pair.

    For "count" type, appends "_total" so otel_etl classifies it as counter.
    For histogram sub-metrics (e.g., name ends with ".95percentile"),
    strips the suffix and returns ("gauge", base_name).

    Args:
        dd_type: Datadog type string ("gauge", "count", "rate", "distribution").
        metric_name: Original Datadog metric name (dot-separated).

    Returns:
        (normalized_metric_name, otel_type)
    """
    # Check for histogram sub-metric suffixes, but only when the dd_type
    # is NOT explicitly "count" (to avoid treating "http.request.count"
    # as a histogram sub-metric).
    if dd_type not in ("count", "rate"):
        for suffix, _agg_name in DD_HISTOGRAM_SUFFIXES.items():
            if metric_name.endswith(suffix):
                base_name = metric_name[: -len(suffix)]
                return normalize_dd_metric_name(base_name), "gauge"

    normalized = normalize_dd_metric_name(metric_name)

    type_info = DD_TYPE_MAPPING.get(dd_type, (None, "gauge"))
    name_suffix, otel_type = type_info

    if name_suffix and not normalized.endswith(name_suffix):
        normalized = normalized + name_suffix

    return normalized, otel_type


def tags_and_name_to_otel(
    metric_name: str,
    dd_type: str,
    tags: list[str] | None,
    host: str | None = None,
) -> tuple[str, dict[str, str], str]:
    """One-shot conversion of DD metric info to otel_etl convention.

    Returns:
        (normalized_metric_name, labels_dict, otel_type)
    """
    normalized_name, otel_type = map_dd_metric_type(dd_type, metric_name)
    labels = map_dd_tags_to_otel(parse_dd_tags(tags))

    # Inject host as "instance" if present and not already in labels
    if host and "instance" not in labels:
        labels["instance"] = host

    return normalized_name, labels, otel_type
