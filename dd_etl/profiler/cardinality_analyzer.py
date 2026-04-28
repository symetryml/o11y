"""Analyze tag cardinality using the Datadog API."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from otel_etl.profiler.label_discovery import LabelInfo
from otel_etl.profiler.cardinality_analyzer import CardinalityResult
from otel_etl.config.defaults import (
    CardinalityThresholds,
    DEFAULT_CARDINALITY_THRESHOLDS,
    get_tier,
    get_action,
)

from dd_etl.config.defaults import DD_TAG_MAPPING

logger = logging.getLogger(__name__)

# Reverse mapping: OTel label → DD tag name
_REVERSE_TAG_MAP = {v: k for k, v in DD_TAG_MAPPING.items()}


def analyze_cardinality(
    client,  # DatadogClient
    labels_by_family: dict[str, dict[str, LabelInfo]],
    thresholds: CardinalityThresholds | None = None,
    top_n: int = 20,
    max_workers: int = 4,
) -> dict[str, dict[str, CardinalityResult]]:
    """Analyze cardinality for all labels using Datadog API queries.

    Args:
        client: DatadogClient instance.
        labels_by_family: Output from discover_labels().
        thresholds: Tier thresholds.
        top_n: Number of top values to capture.
        max_workers: Parallel workers.

    Returns:
        family_name → label_name → CardinalityResult
    """
    thresholds = thresholds or DEFAULT_CARDINALITY_THRESHOLDS
    result: dict[str, dict[str, CardinalityResult]] = {}

    tasks = []
    for family_name, family_labels in labels_by_family.items():
        result[family_name] = {}
        for label_name, label_info in family_labels.items():
            # Get first metric to query against (in DD dot format)
            if label_info["metrics"]:
                metric = label_info["metrics"][0].replace("_", ".")
                metric = metric.removesuffix(".total")
            else:
                continue

            # Convert OTel label name back to DD tag name for querying
            dd_tag = _REVERSE_TAG_MAP.get(label_name, label_name)
            tasks.append((family_name, label_name, metric, dd_tag))

    def _analyze_one(task):
        family_name, label_name, metric, dd_tag = task
        try:
            cardinality = client.count_tag_cardinality(metric, dd_tag)
            tier = get_tier(cardinality, thresholds)
            action = get_action(tier)

            top_values = None
            if tier >= 2 and cardinality > 0:
                top_values = client.get_top_n_values(metric, dd_tag, top_n)

            return (
                family_name,
                label_name,
                CardinalityResult(
                    label=label_name,
                    cardinality=cardinality,
                    tier=tier,
                    action=action,
                    top_values=top_values,
                ),
            )
        except Exception as e:
            logger.warning(f"Failed cardinality for {family_name}.{label_name}: {e}")
            return (
                family_name,
                label_name,
                CardinalityResult(
                    label=label_name,
                    cardinality=0,
                    tier=4,
                    action="drop",
                    top_values=None,
                ),
            )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_analyze_one, t): t for t in tasks}
        for future in as_completed(futures):
            family_name, label_name, cr = future.result()
            result[family_name][label_name] = cr

    tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    for fam in result.values():
        for cr in fam.values():
            tier_counts[cr["tier"]] += 1

    logger.info(
        f"Cardinality analysis: T1={tier_counts[1]}, T2={tier_counts[2]}, "
        f"T3={tier_counts[3]}, T4={tier_counts[4]}"
    )
    return result
