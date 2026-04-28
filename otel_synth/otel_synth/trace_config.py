# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Configuration and profile dataclasses for trace and log synthesis."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from otel_synth.config import SeriesStats


# ---------------------------------------------------------------------------
# Attribute profile
# ---------------------------------------------------------------------------


@dataclass
class AttributeProfile:
    """How to generate one span attribute."""

    key: str = ""
    strategy: str = "constant"  # "constant", "categorical", "uuid", "numeric", "product_id"
    constant_value: str | None = None
    categorical_values: list[str] | None = None
    categorical_weights: list[float] | None = None
    numeric_stats: SeriesStats | None = None


# ---------------------------------------------------------------------------
# Span event profile
# ---------------------------------------------------------------------------


@dataclass
class SpanEventProfile:
    """Profiled span event (name + timing offset from span start)."""

    name: str = ""
    relative_offset_us: SeriesStats = field(default_factory=SeriesStats)
    attributes: list[AttributeProfile] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Log field and template profiles
# ---------------------------------------------------------------------------


@dataclass
class LogFieldProfile:
    """How to fill a single placeholder in a log body template."""

    placeholder: str = ""  # e.g., "<UUID>", "<PRODUCT_ID>"
    strategy: str = "uuid"  # "uuid", "product_id", "ip", "number", "amount", "categorical"
    categorical_values: list[str] | None = None
    categorical_weights: list[float] | None = None
    numeric_stats: SeriesStats | None = None


@dataclass
class LogTemplateProfile:
    """A log message template associated with a specific span type."""

    template_key: str = ""
    service_name: str = ""
    severity: str = "INFO"  # "INFO", "ERROR", "WARN", "DEBUG"
    body_template: str = ""  # "Processing checkout for user <UUID>"
    body_fields: list[LogFieldProfile] = field(default_factory=list)
    emission_rate: float = 1.0  # P(this log is emitted | span of matching type occurs)
    associated_span_operation: str = ""
    associated_span_service: str = ""

    # Anomaly deltas
    delta_emission_rate: float | None = None
    existence: str = "both"  # "both", "emergent", "disappeared"


# ---------------------------------------------------------------------------
# Child edge
# ---------------------------------------------------------------------------


@dataclass
class ChildEdge:
    """An edge from parent span to child span(s) in the template tree."""

    child: SpanProfile | None = None
    relation: str = "sequential"  # "sequential" or "parallel"
    is_repeatable: bool = False
    repeat_count_stats: SeriesStats | None = None
    is_group: bool = False
    group_children: list[SpanProfile] | None = None


# ---------------------------------------------------------------------------
# Span profile
# ---------------------------------------------------------------------------


@dataclass
class SpanProfile:
    """One span position in a trace template tree."""

    service_name: str = ""
    operation_name: str = ""
    span_kind: str = "SPAN_KIND_SERVER"
    duration_us: SeriesStats = field(default_factory=SeriesStats)
    error_rate: float = 0.0
    status_message_catalog: list[str] = field(default_factory=list)
    attributes: list[AttributeProfile] = field(default_factory=list)
    children: list[ChildEdge] = field(default_factory=list)
    log_template_refs: list[str] = field(default_factory=list)
    span_events: list[SpanEventProfile] = field(default_factory=list)

    # Anomaly deltas (None for baseline)
    delta_duration_mean: float | None = None
    delta_duration_std: float | None = None
    delta_error_rate: float | None = None


# ---------------------------------------------------------------------------
# Trace template
# ---------------------------------------------------------------------------


@dataclass
class TraceTemplate:
    """A complete trace topology (one request type, one error variant)."""

    template_name: str = ""
    base_template_name: str = ""
    signature: str = ""
    is_error_variant: bool = False
    error_span_positions: list[str] | None = None
    root_span: SpanProfile = field(default_factory=SpanProfile)
    weight: float = 0.0
    n_instances: int = 0
    gap_fraction_stats: SeriesStats = field(default_factory=SeriesStats)
    resource_attributes: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Trace profile metadata
# ---------------------------------------------------------------------------


@dataclass
class TraceProfileMetadata:
    """Metadata about a profiled trace regime."""

    regime_name: str = ""
    is_baseline: bool = True
    duration_seconds: float = 0.0
    source_traces_csv: str = ""
    source_logs_csv: str = ""
    n_traces: int = 0
    n_spans: int = 0
    n_templates: int = 0
    n_log_templates: int = 0


# ---------------------------------------------------------------------------
# Trace regime profile (the saved artifact)
# ---------------------------------------------------------------------------


@dataclass
class TraceRegimeProfile:
    """Complete trace + log profile for one regime."""

    metadata: TraceProfileMetadata = field(default_factory=TraceProfileMetadata)
    trace_templates: dict[str, TraceTemplate] = field(default_factory=dict)
    log_templates: dict[str, LogTemplateProfile] = field(default_factory=dict)
    total_request_rate: float = 1.0  # traces per second
    template_weights: dict[str, float] = field(default_factory=dict)

    # Anomaly deltas
    delta_request_rate: float | None = None
    delta_template_weights: dict[str, float] | None = None

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2, default=str)

    @classmethod
    def load(cls, path: str | Path) -> TraceRegimeProfile:
        with open(path) as f:
            data = json.load(f)
        return _dict_to_trace_regime_profile(data)


# ---------------------------------------------------------------------------
# Deserialization helpers
# ---------------------------------------------------------------------------


def _dict_to_series_stats(data: dict[str, Any] | None) -> SeriesStats | None:
    """Reconstruct a SeriesStats from a JSON-loaded dict."""
    if data is None:
        return None
    return SeriesStats(**data)


def _dict_to_attribute_profile(data: dict[str, Any]) -> AttributeProfile:
    ns_raw = data.pop("numeric_stats", None)
    ns = SeriesStats(**ns_raw) if ns_raw else None
    return AttributeProfile(numeric_stats=ns, **data)


def _dict_to_span_event_profile(data: dict[str, Any]) -> SpanEventProfile:
    offset_raw = data.pop("relative_offset_us", {})
    offset = SeriesStats(**offset_raw) if offset_raw else SeriesStats()
    attrs_raw = data.pop("attributes", [])
    attrs = [_dict_to_attribute_profile(a) for a in attrs_raw]
    return SpanEventProfile(relative_offset_us=offset, attributes=attrs, **data)


def _dict_to_log_field_profile(data: dict[str, Any]) -> LogFieldProfile:
    ns_raw = data.pop("numeric_stats", None)
    ns = SeriesStats(**ns_raw) if ns_raw else None
    return LogFieldProfile(numeric_stats=ns, **data)


def _dict_to_log_template_profile(data: dict[str, Any]) -> LogTemplateProfile:
    fields_raw = data.pop("body_fields", [])
    fields = [_dict_to_log_field_profile(f) for f in fields_raw]
    return LogTemplateProfile(body_fields=fields, **data)


def _dict_to_span_profile(data: dict[str, Any]) -> SpanProfile:
    dur_raw = data.pop("duration_us", {})
    dur = SeriesStats(**dur_raw) if dur_raw else SeriesStats()
    attrs_raw = data.pop("attributes", [])
    attrs = [_dict_to_attribute_profile(a) for a in attrs_raw]
    children_raw = data.pop("children", [])
    children = [_dict_to_child_edge(c) for c in children_raw]
    events_raw = data.pop("span_events", [])
    events = [_dict_to_span_event_profile(e) for e in events_raw]
    return SpanProfile(
        duration_us=dur,
        attributes=attrs,
        children=children,
        span_events=events,
        **data,
    )


def _dict_to_child_edge(data: dict[str, Any]) -> ChildEdge:
    child_raw = data.pop("child", None)
    child = _dict_to_span_profile(child_raw) if child_raw else None
    rcs_raw = data.pop("repeat_count_stats", None)
    rcs = SeriesStats(**rcs_raw) if rcs_raw else None
    gc_raw = data.pop("group_children", None)
    gc = [_dict_to_span_profile(g) for g in gc_raw] if gc_raw else None
    return ChildEdge(child=child, repeat_count_stats=rcs, group_children=gc, **data)


def _dict_to_trace_template(data: dict[str, Any]) -> TraceTemplate:
    root_raw = data.pop("root_span", {})
    root = _dict_to_span_profile(root_raw) if root_raw else SpanProfile()
    gap_raw = data.pop("gap_fraction_stats", {})
    gap = SeriesStats(**gap_raw) if gap_raw else SeriesStats()
    return TraceTemplate(root_span=root, gap_fraction_stats=gap, **data)


@dataclass
class LogRegimeProfile:
    """Standalone log profile for one regime — saved as .log.profile.json."""

    metadata: TraceProfileMetadata = field(default_factory=TraceProfileMetadata)
    log_templates: dict[str, LogTemplateProfile] = field(default_factory=dict)

    # Anomaly deltas
    delta_log_templates: dict[str, LogTemplateProfile] | None = None

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2, default=str)

    @classmethod
    def load(cls, path: str | Path) -> LogRegimeProfile:
        with open(path) as f:
            data = json.load(f)
        metadata = TraceProfileMetadata(**data.get("metadata", {}))
        log_templates = {}
        for key, lt_data in data.get("log_templates", {}).items():
            log_templates[key] = _dict_to_log_template_profile(lt_data)
        return LogRegimeProfile(metadata=metadata, log_templates=log_templates)


def _dict_to_trace_regime_profile(data: dict[str, Any]) -> TraceRegimeProfile:
    metadata = TraceProfileMetadata(**data.get("metadata", {}))

    trace_templates = {}
    for key, tt_data in data.get("trace_templates", {}).items():
        trace_templates[key] = _dict_to_trace_template(tt_data)

    log_templates = {}
    for key, lt_data in data.get("log_templates", {}).items():
        log_templates[key] = _dict_to_log_template_profile(lt_data)

    return TraceRegimeProfile(
        metadata=metadata,
        trace_templates=trace_templates,
        log_templates=log_templates,
        total_request_rate=data.get("total_request_rate", 1.0),
        template_weights=data.get("template_weights", {}),
        delta_request_rate=data.get("delta_request_rate"),
        delta_template_weights=data.get("delta_template_weights"),
    )
