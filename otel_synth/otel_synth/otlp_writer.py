# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""OTLP JSON serialization for traces and logs."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path

from otel_synth.trace_generator import GeneratedLog, GeneratedSpan

logger = logging.getLogger(__name__)

# OTLP span kind mapping
_SPAN_KIND_MAP = {
    "SPAN_KIND_INTERNAL": 1,
    "SPAN_KIND_SERVER": 2,
    "SPAN_KIND_CLIENT": 3,
    "SPAN_KIND_PRODUCER": 4,
    "SPAN_KIND_CONSUMER": 5,
}

# OTLP status code mapping
_STATUS_OK = 1
_STATUS_ERROR = 2

# OTLP severity number mapping
_SEVERITY_MAP = {
    "TRACE": 1,
    "DEBUG": 5,
    "INFO": 9,
    "WARN": 13,
    "WARNING": 13,
    "ERROR": 17,
    "FATAL": 21,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_attributes(attrs: dict[str, str]) -> list[dict]:
    """Convert a flat dict to OTLP attribute list."""
    return [
        {"key": k, "value": {"stringValue": str(v)}}
        for k, v in attrs.items()
    ]


def _us_to_nanos(us: int) -> str:
    """Convert microseconds to nanoseconds string (OTLP uses uint64 as string)."""
    return str(us * 1000)


# ---------------------------------------------------------------------------
# Trace OTLP serialization
# ---------------------------------------------------------------------------


def _group_spans_by_resource(
    spans: list[GeneratedSpan],
) -> dict[str, list[GeneratedSpan]]:
    """Group spans by service name + resource attributes for OTLP ResourceSpans."""
    groups: dict[str, list[GeneratedSpan]] = defaultdict(list)
    for span in spans:
        # Key by service name (resource identity)
        key = span.service_name
        groups[key].append(span)
    return groups


def _span_to_otlp(span: GeneratedSpan) -> dict:
    """Convert a GeneratedSpan to OTLP Span JSON."""
    otlp_span: dict = {
        "traceId": span.trace_id,
        "spanId": span.span_id,
        "name": span.operation_name,
        "kind": _SPAN_KIND_MAP.get(span.span_kind, 2),
        "startTimeUnixNano": _us_to_nanos(span.start_time_us),
        "endTimeUnixNano": _us_to_nanos(span.start_time_us + span.duration_us),
    }

    if span.parent_span_id:
        otlp_span["parentSpanId"] = span.parent_span_id

    # Status
    if span.is_error:
        otlp_span["status"] = {"code": _STATUS_ERROR}
        if span.status_message:
            otlp_span["status"]["message"] = span.status_message
    else:
        otlp_span["status"] = {"code": _STATUS_OK}

    # Attributes
    if span.attributes:
        otlp_span["attributes"] = _make_attributes(span.attributes)

    # Events
    if span.events:
        otlp_span["events"] = span.events

    return otlp_span


def spans_to_otlp(spans: list[GeneratedSpan]) -> list[dict]:
    """Convert a list of GeneratedSpans to OTLP ResourceSpans JSON objects.

    Returns a list of dicts, each representing one OTLP export payload
    (one per service/resource).
    """
    resource_groups = _group_spans_by_resource(spans)
    payloads = []

    for service_name, group_spans in resource_groups.items():
        # Get resource attributes from first span
        resource_attrs = {"service.name": service_name}
        if group_spans and group_spans[0].resource_attributes:
            resource_attrs.update(group_spans[0].resource_attributes)

        # Group by trace_id for scope organization
        by_trace: dict[str, list[GeneratedSpan]] = defaultdict(list)
        for s in group_spans:
            by_trace[s.trace_id].append(s)

        otlp_spans = [_span_to_otlp(s) for s in group_spans]

        payload = {
            "resourceSpans": [{
                "resource": {
                    "attributes": _make_attributes(resource_attrs),
                },
                "scopeSpans": [{
                    "scope": {"name": f"otel_synth.{service_name}"},
                    "spans": otlp_spans,
                }],
            }],
        }
        payloads.append(payload)

    return payloads


# ---------------------------------------------------------------------------
# Log OTLP serialization
# ---------------------------------------------------------------------------


def _group_logs_by_resource(
    logs: list[GeneratedLog],
) -> dict[str, list[GeneratedLog]]:
    """Group logs by service name for OTLP ResourceLogs."""
    groups: dict[str, list[GeneratedLog]] = defaultdict(list)
    for log in logs:
        groups[log.service_name].append(log)
    return groups


def _log_to_otlp(log: GeneratedLog) -> dict:
    """Convert a GeneratedLog to OTLP LogRecord JSON."""
    record: dict = {
        "timeUnixNano": _us_to_nanos(log.timestamp_us),
        "severityNumber": _SEVERITY_MAP.get(log.severity.upper(), 9),
        "severityText": log.severity,
        "body": {"stringValue": log.body},
    }

    if log.trace_id:
        record["traceId"] = log.trace_id
    if log.span_id:
        record["spanId"] = log.span_id
    if log.attributes:
        record["attributes"] = _make_attributes(log.attributes)

    return record


def logs_to_otlp(logs: list[GeneratedLog]) -> list[dict]:
    """Convert a list of GeneratedLogs to OTLP ResourceLogs JSON objects.

    Returns a list of dicts, each representing one OTLP export payload
    (one per service/resource).
    """
    resource_groups = _group_logs_by_resource(logs)
    payloads = []

    for service_name, group_logs in resource_groups.items():
        resource_attrs = {"service.name": service_name}
        if group_logs and group_logs[0].resource_attributes:
            resource_attrs.update(group_logs[0].resource_attributes)

        log_records = [_log_to_otlp(l) for l in group_logs]

        payload = {
            "resourceLogs": [{
                "resource": {
                    "attributes": _make_attributes(resource_attrs),
                },
                "scopeLogs": [{
                    "scope": {"name": f"otel_synth.{service_name}"},
                    "logRecords": log_records,
                }],
            }],
        }
        payloads.append(payload)

    return payloads


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------


def write_traces_otlp(
    spans: list[GeneratedSpan],
    output_path: str | Path,
) -> None:
    """Write spans as OTLP JSON (one JSON object per line)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payloads = spans_to_otlp(spans)

    with open(output_path, "w") as f:
        for payload in payloads:
            f.write(json.dumps(payload, separators=(",", ":")) + "\n")

    logger.info(f"Wrote {len(spans)} spans ({len(payloads)} resource groups) to {output_path}")


def write_logs_otlp(
    logs: list[GeneratedLog],
    output_path: str | Path,
) -> None:
    """Write logs as OTLP JSON (one JSON object per line)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payloads = logs_to_otlp(logs)

    with open(output_path, "w") as f:
        for payload in payloads:
            f.write(json.dumps(payload, separators=(",", ":")) + "\n")

    logger.info(f"Wrote {len(logs)} logs ({len(payloads)} resource groups) to {output_path}")


def write_traces_csv(
    spans: list[GeneratedSpan],
    output_path: str | Path,
) -> None:
    """Write spans as flat CSV for analysis."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write("trace_id,span_id,parent_span_id,operation_name,service_name,start_time_us,duration_us,is_error,status_message\n")
        for s in spans:
            f.write(
                f"{s.trace_id},{s.span_id},{s.parent_span_id},"
                f"{s.operation_name},{s.service_name},"
                f"{s.start_time_us},{s.duration_us},"
                f"{s.is_error},{s.status_message}\n"
            )

    logger.info(f"Wrote {len(spans)} spans to {output_path}")


def write_logs_csv(
    logs: list[GeneratedLog],
    output_path: str | Path,
) -> None:
    """Write logs as flat CSV for analysis."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write("timestamp_us,service,severity,body,trace_id,span_id\n")
        for l in logs:
            # Escape commas in body
            body = l.body.replace('"', '""')
            f.write(
                f"{l.timestamp_us},{l.service_name},{l.severity},"
                f'"{body}",{l.trace_id},{l.span_id}\n'
            )

    logger.info(f"Wrote {len(logs)} logs to {output_path}")
