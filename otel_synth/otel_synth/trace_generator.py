# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Phase 2G: Generate synthetic traces + logs from trace/log profiles."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np

from otel_synth.config import SeriesStats
from otel_synth.trace_config import (
    ChildEdge,
    LogTemplateProfile,
    SpanProfile,
    TraceRegimeProfile,
    TraceTemplate,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output data structures
# ---------------------------------------------------------------------------


@dataclass
class GeneratedSpan:
    """A single generated span."""

    trace_id: str
    span_id: str
    parent_span_id: str
    service_name: str
    operation_name: str
    span_kind: str
    start_time_us: int
    duration_us: int
    is_error: bool
    status_message: str = ""
    attributes: dict[str, str] = field(default_factory=dict)
    resource_attributes: dict[str, str] = field(default_factory=dict)
    events: list[dict] = field(default_factory=list)


@dataclass
class GeneratedLog:
    """A single generated log record."""

    timestamp_us: int
    service_name: str
    severity: str
    body: str
    trace_id: str = ""
    span_id: str = ""
    attributes: dict[str, str] = field(default_factory=dict)
    resource_attributes: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Attribute generation
# ---------------------------------------------------------------------------


def _generate_attribute_value(attr, rng: np.random.Generator) -> str:
    """Generate a single attribute value from an AttributeProfile."""
    if attr.strategy == "constant":
        return attr.constant_value or ""
    elif attr.strategy == "uuid":
        return str(uuid.uuid4())
    elif attr.strategy == "product_id":
        # Sample from known OTel demo product IDs
        products = [
            "OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", "L9ECAV7KIM",
            "2ZYFJ3GM2N", "0PUK6V6EV0", "LS4PSXUNUM", "9SIQT8TOJO",
            "6E92ZMYYFZ", "HQTGWGPNH4",
        ]
        return rng.choice(products)
    elif attr.strategy == "categorical":
        if attr.categorical_values and attr.categorical_weights:
            return rng.choice(attr.categorical_values, p=attr.categorical_weights)
        return ""
    elif attr.strategy == "numeric":
        if attr.numeric_stats:
            val = rng.normal(attr.numeric_stats.mean, max(attr.numeric_stats.std, 1e-6))
            return str(round(val, 2))
        return "0"
    return ""


def _generate_attributes(
    attr_profiles: list, rng: np.random.Generator
) -> dict[str, str]:
    """Generate all attributes for a span."""
    attrs = {}
    for ap in attr_profiles:
        attrs[ap.key] = _generate_attribute_value(ap, rng)
    return attrs


# ---------------------------------------------------------------------------
# Duration sampling
# ---------------------------------------------------------------------------


def _sample_duration(stats: SeriesStats, rng: np.random.Generator) -> float:
    """Sample a duration from SeriesStats, ensuring >= 0."""
    if stats.n_points == 0:
        return max(stats.mean, 1.0)
    val = rng.normal(stats.mean, max(stats.std, 1e-6))
    return max(val, max(stats.min, 1.0))


def _sample_gap_fraction(gap_stats: SeriesStats, rng: np.random.Generator) -> float:
    """Sample a gap fraction (clamped to [0.01, 0.99])."""
    if gap_stats.n_points == 0:
        return 0.1
    val = rng.normal(gap_stats.mean, max(gap_stats.std, 0.01))
    return float(np.clip(val, 0.01, 0.99))


# ---------------------------------------------------------------------------
# Log generation
# ---------------------------------------------------------------------------


def _fill_log_template(template: str, fields: list, rng: np.random.Generator) -> str:
    """Fill a log body template with generated values."""
    body = template
    for f in fields:
        placeholder = f.placeholder
        if f.strategy == "uuid":
            replacement = str(uuid.uuid4())
        elif f.strategy == "product_id":
            products = [
                "OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", "L9ECAV7KIM",
                "2ZYFJ3GM2N", "0PUK6V6EV0", "LS4PSXUNUM", "9SIQT8TOJO",
            ]
            replacement = rng.choice(products)
        elif f.strategy == "ip":
            replacement = f"{rng.integers(1, 255)}.{rng.integers(0, 255)}.{rng.integers(0, 255)}.{rng.integers(1, 255)}"
        elif f.strategy == "number":
            replacement = str(rng.integers(0, 10000))
        elif f.strategy == "amount":
            replacement = f"{rng.uniform(0.01, 999.99):.2f}"
        elif f.strategy == "categorical":
            if f.categorical_values and f.categorical_weights:
                replacement = rng.choice(f.categorical_values, p=f.categorical_weights)
            else:
                replacement = ""
        else:
            replacement = str(uuid.uuid4())
        # Replace first occurrence only
        body = body.replace(placeholder, replacement, 1)
    return body


def _generate_log_for_span(
    log_template: LogTemplateProfile,
    trace_id: str,
    span_id: str,
    span_start_us: int,
    span_duration_us: int,
    resource_attributes: dict[str, str],
    rng: np.random.Generator,
) -> GeneratedLog | None:
    """Conditionally generate a log record for a span."""
    effective_rate = log_template.emission_rate
    if log_template.delta_emission_rate is not None:
        effective_rate = max(0.0, min(1.0, effective_rate))

    if rng.random() >= effective_rate:
        return None

    # Log timestamp within span window
    offset = rng.uniform(0, max(span_duration_us, 1))
    log_ts = span_start_us + int(offset)

    body = _fill_log_template(log_template.body_template, log_template.body_fields, rng)

    return GeneratedLog(
        timestamp_us=log_ts,
        service_name=log_template.service_name,
        severity=log_template.severity,
        body=body,
        trace_id=trace_id,
        span_id=span_id,
        resource_attributes=resource_attributes,
    )


# ---------------------------------------------------------------------------
# Span tree generation (recursive, top-down with budget allocation)
# ---------------------------------------------------------------------------


def _generate_span_tree(
    span_profile: SpanProfile,
    trace_id: str,
    parent_span_id: str,
    parent_start_us: int,
    parent_budget_us: float,
    gap_stats: SeriesStats,
    log_templates: dict[str, LogTemplateProfile],
    resource_attributes: dict[str, str],
    rng: np.random.Generator,
) -> tuple[list[GeneratedSpan], list[GeneratedLog]]:
    """Recursively generate a span tree with budget allocation.

    Returns (spans, logs).
    """
    span_id = uuid.uuid4().hex[:16]

    # Error decision
    is_error = rng.random() < span_profile.error_rate
    status_message = ""
    if is_error and span_profile.status_message_catalog:
        status_message = rng.choice(span_profile.status_message_catalog)

    # Generate attributes
    attrs = _generate_attributes(span_profile.attributes, rng)

    # Resolve children
    child_specs: list[tuple[SpanProfile, str]] = []  # (child_profile, relation)
    for edge in span_profile.children:
        if edge.is_repeatable and edge.repeat_count_stats is not None:
            n_repeat = max(0, round(_sample_duration(edge.repeat_count_stats, rng)))
        elif edge.is_repeatable:
            n_repeat = 1
        else:
            n_repeat = 1

        if edge.is_group and edge.group_children:
            for _ in range(n_repeat):
                for group_child in edge.group_children:
                    child_specs.append((group_child, edge.relation))
        elif edge.child is not None:
            for _ in range(n_repeat):
                child_specs.append((edge.child, edge.relation))

    # Sample raw durations for children
    child_raw_durations = [
        _sample_duration(cp.duration_us, rng) for cp, _ in child_specs
    ]

    # Classify sequential vs parallel
    sequential = [(d, cp, r) for d, (cp, r) in zip(child_raw_durations, child_specs) if r == "sequential"]
    parallel = [(d, cp, r) for d, (cp, r) in zip(child_raw_durations, child_specs) if r == "parallel"]

    total_sequential = sum(d for d, _, _ in sequential)
    max_parallel = max((d for d, _, _ in parallel), default=0)
    total_child_request = total_sequential + max_parallel

    # Sample gap fraction
    gap_frac = _sample_gap_fraction(gap_stats, rng)

    # Determine this span's duration
    raw_duration = _sample_duration(span_profile.duration_us, rng)
    target_duration = max(raw_duration, parent_budget_us) if parent_budget_us > 0 else raw_duration
    if total_child_request > 0:
        target_duration = max(target_duration, total_child_request / (1.0 - gap_frac + 1e-9))

    gap = target_duration * gap_frac

    # Budget available for children
    available = target_duration - gap
    if total_child_request > available and total_child_request > 0:
        scale = available / total_child_request
        child_raw_durations = [d * scale for d in child_raw_durations]

    # Assign child timestamps
    child_spans: list[GeneratedSpan] = []
    child_logs: list[GeneratedLog] = []
    cursor = parent_start_us + gap * 0.1  # small initial gap

    for i, (child_prof, relation) in enumerate(child_specs):
        child_budget = child_raw_durations[i]
        if relation == "parallel":
            child_start = parent_start_us + int(gap * 0.1)
        else:
            child_start = int(cursor)

        sub_spans, sub_logs = _generate_span_tree(
            child_prof,
            trace_id,
            span_id,
            int(child_start),
            child_budget,
            gap_stats,
            log_templates,
            resource_attributes,
            rng,
        )
        child_spans.extend(sub_spans)
        child_logs.extend(sub_logs)

        if relation == "sequential":
            cursor = child_start + child_budget

    # Final duration: max of target and actual child extent
    actual_duration = target_duration
    if child_spans:
        max_child_end = max(s.start_time_us + s.duration_us for s in child_spans)
        extent = max_child_end - parent_start_us
        actual_duration = max(target_duration, extent)

    # Build this span
    this_span = GeneratedSpan(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        service_name=span_profile.service_name,
        operation_name=span_profile.operation_name,
        span_kind=span_profile.span_kind,
        start_time_us=parent_start_us,
        duration_us=int(actual_duration),
        is_error=is_error,
        status_message=status_message,
        attributes=attrs,
        resource_attributes=resource_attributes,
    )

    # Generate associated logs
    for ref in span_profile.log_template_refs:
        if ref in log_templates:
            log = _generate_log_for_span(
                log_templates[ref],
                trace_id, span_id,
                parent_start_us, int(actual_duration),
                resource_attributes,
                rng,
            )
            if log is not None:
                child_logs.append(log)

    return [this_span] + child_spans, child_logs


# ---------------------------------------------------------------------------
# Public API: generate_traces
# ---------------------------------------------------------------------------


def generate_traces(
    profile: TraceRegimeProfile,
    start_time_us: int,
    duration_us: int,
    step_seconds: int = 60,
    rng: np.random.Generator | None = None,
) -> tuple[list[GeneratedSpan], list[GeneratedLog]]:
    """Generate synthetic traces + logs from a TraceRegimeProfile.

    Args:
        profile: the trace/log profile to generate from
        start_time_us: start timestamp in microseconds
        duration_us: total duration in microseconds
        step_seconds: time step for Poisson sampling
        rng: random number generator

    Returns:
        (spans, logs) — lists of generated spans and logs
    """
    if rng is None:
        rng = np.random.default_rng()

    if not profile.trace_templates:
        logger.warning("No trace templates in profile — returning empty")
        return [], []

    step_us = step_seconds * 1_000_000
    n_steps = max(1, int(duration_us / step_us))

    # Effective request rate
    request_rate = profile.total_request_rate
    if profile.delta_request_rate is not None:
        request_rate = max(0.01, request_rate)

    # Effective template weights
    weights = dict(profile.template_weights)
    if profile.delta_template_weights:
        for tname, dw in profile.delta_template_weights.items():
            if tname in weights:
                weights[tname] = max(0.0, weights[tname] + dw)
        # Renormalize
        total_w = sum(weights.values())
        if total_w > 0:
            weights = {k: v / total_w for k, v in weights.items()}

    template_names = list(weights.keys())
    template_probs = np.array([weights[n] for n in template_names])
    if template_probs.sum() == 0:
        template_probs = np.ones(len(template_names)) / len(template_names)
    else:
        template_probs = template_probs / template_probs.sum()

    all_spans: list[GeneratedSpan] = []
    all_logs: list[GeneratedLog] = []

    for step_i in range(n_steps):
        step_start = start_time_us + step_i * step_us

        # Poisson number of traces this step
        expected = request_rate * step_seconds
        n_traces = rng.poisson(expected)

        for _ in range(n_traces):
            # Pick template
            tmpl_idx = rng.choice(len(template_names), p=template_probs)
            tmpl_name = template_names[tmpl_idx]
            tmpl = profile.trace_templates[tmpl_name]

            # Generate trace
            trace_id = uuid.uuid4().hex
            trace_start = step_start + int(rng.uniform(0, step_us))

            spans, logs = _generate_span_tree(
                tmpl.root_span,
                trace_id,
                "",  # root has no parent
                trace_start,
                0,  # no budget constraint on root
                tmpl.gap_fraction_stats,
                profile.log_templates,
                tmpl.resource_attributes,
                rng,
            )
            all_spans.extend(spans)
            all_logs.extend(logs)

    # Generate non-trace logs (standalone logs not associated with spans)
    for lt_key, lt in profile.log_templates.items():
        if lt.associated_span_operation:
            continue  # already generated with spans
        if lt.existence == "disappeared":
            continue
        # Standalone logs: emit at fixed rate per step
        for step_i in range(n_steps):
            step_start = start_time_us + step_i * step_us
            if rng.random() < lt.emission_rate:
                body = _fill_log_template(lt.body_template, lt.body_fields, rng)
                log_ts = step_start + int(rng.uniform(0, step_us))
                all_logs.append(GeneratedLog(
                    timestamp_us=log_ts,
                    service_name=lt.service_name,
                    severity=lt.severity,
                    body=body,
                ))

    logger.info(f"Generated {len(all_spans)} spans, {len(all_logs)} logs")
    return all_spans, all_logs


# ---------------------------------------------------------------------------
# Derive metrics from generated traces (Tier 1)
# ---------------------------------------------------------------------------


def derive_trace_metrics(
    spans: list[GeneratedSpan],
    start_time_us: int,
    step_us: int,
    n_steps: int,
) -> list[dict]:
    """Derive trace-correlated metrics from generated spans.

    Produces:
    - request_count_total per service per status
    - request_duration histogram per service (simplified as gauge percentiles)
    - error_count_total per service

    Returns list of metric rows: {timestamp, metric, labels, value}
    """
    if not spans:
        return []

    # Group spans by step and service
    step_service_data: dict[int, dict[str, dict]] = {}

    for span in spans:
        step_idx = min((span.start_time_us - start_time_us) // step_us, n_steps - 1)
        step_idx = max(0, step_idx)
        svc = span.service_name

        if step_idx not in step_service_data:
            step_service_data[step_idx] = {}
        if svc not in step_service_data[step_idx]:
            step_service_data[step_idx][svc] = {
                "request_count": 0,
                "error_count": 0,
                "durations": [],
            }

        data = step_service_data[step_idx][svc]
        data["request_count"] += 1
        if span.is_error:
            data["error_count"] += 1
        data["durations"].append(span.duration_us / 1_000_000)  # convert to seconds

    rows = []
    # Cumulative counters
    cumulative_requests: dict[str, float] = {}
    cumulative_errors: dict[str, float] = {}

    for step_i in range(n_steps):
        ts_us = start_time_us + step_i * step_us
        ts_str = datetime.utcfromtimestamp(ts_us / 1_000_000).strftime("%Y-%m-%d %H:%M:%S")

        step_data = step_service_data.get(step_i, {})

        # Collect all services seen so far
        all_services = set(cumulative_requests.keys()) | set(step_data.keys())

        for svc in all_services:
            svc_data = step_data.get(svc, {"request_count": 0, "error_count": 0, "durations": []})

            # Update cumulative counters
            cumulative_requests[svc] = cumulative_requests.get(svc, 0) + svc_data["request_count"]
            cumulative_errors[svc] = cumulative_errors.get(svc, 0) + svc_data["error_count"]

            labels = str({"service_name": svc})

            # request_count_total
            rows.append({
                "timestamp": ts_str,
                "metric": "trace_request_count_total",
                "labels": labels,
                "value": cumulative_requests[svc],
            })

            # error_count_total
            rows.append({
                "timestamp": ts_str,
                "metric": "trace_error_count_total",
                "labels": labels,
                "value": cumulative_errors[svc],
            })

            # Duration percentiles as gauges (p50, p95, p99)
            durations = svc_data["durations"]
            if durations:
                for pct, label in [(50, "p50"), (95, "p95"), (99, "p99")]:
                    val = float(np.percentile(durations, pct))
                    rows.append({
                        "timestamp": ts_str,
                        "metric": "trace_request_duration_seconds",
                        "labels": str({"service_name": svc, "quantile": label}),
                        "value": val,
                    })

    return rows
