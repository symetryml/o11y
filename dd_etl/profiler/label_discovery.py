"""Discover tags (labels) for each metric family from Datadog API."""

from __future__ import annotations

import logging

from otel_etl.profiler.metric_discovery import MetricFamily
from otel_etl.profiler.label_discovery import LabelInfo

from dd_etl.utils.tag_mapper import map_dd_tags_to_otel
from dd_etl.config.defaults import DD_TAG_MAPPING

logger = logging.getLogger(__name__)


def discover_labels(
    client,  # DatadogClient
    families: dict[str, MetricFamily],
) -> dict[str, dict[str, LabelInfo]]:
    """Discover tags for each metric family using the Datadog API.

    For each family, queries tags via ``client.get_tags_for_metric()``,
    maps DD tag names to OTel-style label names, and returns the
    ``LabelInfo`` dict expected by otel_etl's cardinality analyzer.

    Args:
        client: DatadogClient instance.
        families: Metric families from discover_metrics().

    Returns:
        family_name → label_name → LabelInfo
    """
    result: dict[str, dict[str, LabelInfo]] = {}

    # We need the original DD metric name (not normalized) to query tags.
    # Since we only have normalized names in families, we query using the
    # first metric in each family.  For the API call we need the *original*
    # DD name — we reverse-normalize by replacing _ with . as a heuristic.
    # A better approach would be to carry the original name, but this
    # keeps the implementation simple for now.

    for family_name, family in families.items():
        family_labels: dict[str, LabelInfo] = {}

        # Try to get tags for the first metric in the family
        # Use the family name reversed to dots as the DD metric name
        dd_metric_name = family_name.replace("_", ".")

        try:
            raw_tags = client.get_tags_for_metric(dd_metric_name)
        except Exception as e:
            logger.warning(f"Failed to get tags for {dd_metric_name}: {e}")
            # Try with the first metric name
            if family["metrics"]:
                alt_name = family["metrics"][0].replace("_", ".")
                alt_name = alt_name.removesuffix(".total")
                try:
                    raw_tags = client.get_tags_for_metric(alt_name)
                except Exception:
                    raw_tags = []
            else:
                raw_tags = []

        # Map DD tag names to OTel-style label names
        mapped_tags = {DD_TAG_MAPPING.get(t, t): t for t in raw_tags}

        for otel_label, _dd_tag in mapped_tags.items():
            # Skip histogram-internal labels
            if otel_label in ("le", "quantile"):
                continue

            family_labels[otel_label] = LabelInfo(
                name=otel_label,
                metrics=list(family["metrics"]),
            )

        result[family_name] = family_labels

        if family_labels:
            logger.debug(
                f"Family {family_name}: {len(family_labels)} labels — "
                f"{list(family_labels.keys())}"
            )

    total = sum(len(v) for v in result.values())
    logger.info(f"Discovered {total} labels across {len(result)} families")
    return result
