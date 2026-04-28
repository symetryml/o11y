# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Span duration profiling and attribute categorization."""

from __future__ import annotations

import re
import warnings
from collections import Counter

import numpy as np
from scipy import stats as sp_stats

from otel_synth.config import SeriesStats
from otel_synth.trace_config import AttributeProfile, SpanEventProfile

# Patterns for attribute strategy detection
UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
# OTel product IDs from the demo: 10-char hex-like strings
PRODUCT_ID_PATTERN = re.compile(r"^[A-Z0-9]{10}$")

MAX_CATEGORICAL = 200


def compute_duration_stats(durations_us: np.ndarray) -> SeriesStats:
    """Compute SeriesStats from an array of span durations in microseconds."""
    if len(durations_us) == 0:
        return SeriesStats()
    if len(durations_us) == 1:
        return SeriesStats(
            mean=float(durations_us[0]),
            std=0.0,
            min=float(durations_us[0]),
            max=float(durations_us[0]),
            n_points=1,
        )

    mean = float(np.mean(durations_us))
    std = float(np.std(durations_us, ddof=1))

    # Autocorrelation at lag 1 (for time-ordered durations)
    autocorr = 0.0
    if std > 0 and len(durations_us) > 2:
        centered = durations_us - mean
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            autocorr = float(np.corrcoef(centered[:-1], centered[1:])[0, 1])
        if not np.isfinite(autocorr):
            autocorr = 0.0

    # Skewness and kurtosis
    skew = 0.0
    kurt = 0.0
    if len(durations_us) > 3 and std > 1e-12:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            skew = float(sp_stats.skew(durations_us))
            kurt = float(sp_stats.kurtosis(durations_us))
        if not np.isfinite(skew):
            skew = 0.0
        if not np.isfinite(kurt):
            kurt = 0.0

    return SeriesStats(
        mean=mean,
        std=std,
        min=float(np.min(durations_us)),
        max=float(np.max(durations_us)),
        skewness=skew,
        kurtosis=kurt,
        autocorrelation_lag1=autocorr,
        n_points=len(durations_us),
    )


def categorize_attribute(key: str, values: list[str]) -> AttributeProfile:
    """Categorize an attribute based on observed values and build an AttributeProfile."""
    if not values:
        return AttributeProfile(key=key, strategy="constant", constant_value="")

    unique = list(dict.fromkeys(values))  # preserve order, deduplicate
    n_unique = len(unique)

    # Check UUID pattern before constant (always generate fresh UUIDs)
    if all(UUID_PATTERN.match(v) for v in unique[:50]):
        return AttributeProfile(key=key, strategy="uuid")

    # Check product ID pattern before constant
    if all(PRODUCT_ID_PATTERN.match(v) for v in unique[:50]):
        return AttributeProfile(key=key, strategy="product_id")

    # Single value → constant
    if n_unique == 1:
        return AttributeProfile(key=key, strategy="constant", constant_value=unique[0])

    # Try numeric
    try:
        numeric_vals = np.array([float(v) for v in values])
        return AttributeProfile(
            key=key,
            strategy="numeric",
            numeric_stats=compute_duration_stats(numeric_vals),
        )
    except (ValueError, TypeError):
        pass

    # Categorical
    counts = Counter(values)
    if n_unique > MAX_CATEGORICAL:
        # Keep top-200 and collapse rest to __other__
        top = counts.most_common(MAX_CATEGORICAL)
        other_count = sum(c for _, c in counts.items()) - sum(c for _, c in top)
        cat_values = [v for v, _ in top] + ["__other__"]
        cat_counts = [c for _, c in top] + [other_count]
    else:
        cat_values = list(counts.keys())
        cat_counts = [counts[v] for v in cat_values]

    total = sum(cat_counts)
    cat_weights = [c / total for c in cat_counts]

    return AttributeProfile(
        key=key,
        strategy="categorical",
        categorical_values=cat_values,
        categorical_weights=cat_weights,
    )


def profile_span_events(
    events_per_span: list[list[dict]],
    span_durations_us: np.ndarray,
    span_starts_us: np.ndarray,
) -> list[SpanEventProfile]:
    """Profile span events across multiple instances of the same span position.

    Each entry in events_per_span is a list of event dicts for one span instance:
    [{"name": str, "timestamp_us": int, "attributes": dict}, ...]
    """
    if not events_per_span:
        return []

    # Group events by name across all instances
    events_by_name: dict[str, list[float]] = {}
    events_attrs_by_name: dict[str, dict[str, list[str]]] = {}

    for i, events in enumerate(events_per_span):
        span_start = span_starts_us[i] if i < len(span_starts_us) else 0
        for event in events:
            name = event.get("name", "")
            ts = event.get("timestamp_us", span_start)
            offset = ts - span_start
            events_by_name.setdefault(name, []).append(float(offset))

            # Collect attribute values
            attrs = event.get("attributes", {})
            if name not in events_attrs_by_name:
                events_attrs_by_name[name] = {}
            for k, v in attrs.items():
                events_attrs_by_name[name].setdefault(k, []).append(str(v))

    profiles = []
    for name, offsets in events_by_name.items():
        offset_arr = np.array(offsets)
        attr_profiles = []
        for k, vals in events_attrs_by_name.get(name, {}).items():
            attr_profiles.append(categorize_attribute(k, vals))

        profiles.append(SpanEventProfile(
            name=name,
            relative_offset_us=compute_duration_stats(offset_arr),
            attributes=attr_profiles,
        ))

    return profiles
