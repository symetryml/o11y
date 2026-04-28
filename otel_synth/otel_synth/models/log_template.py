# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Log message template extraction (regex v1, pluggable)."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass

from otel_synth.trace_config import LogFieldProfile

# Normalization patterns, applied in order
_NORMALIZERS: list[tuple[re.Pattern, str, str]] = [
    # UUIDs: 8-4-4-4-12 hex
    (re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I),
     "<UUID>", "uuid"),
    # Hex trace/span IDs (16 or 32 hex chars, standalone)
    (re.compile(r"\b[0-9a-f]{32}\b", re.I), "<TRACE_ID>", "uuid"),
    (re.compile(r"\b[0-9a-f]{16}\b", re.I), "<SPAN_ID>", "uuid"),
    # IP addresses (IPv4)
    (re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"), "<IP>", "ip"),
    # Floating point amounts (e.g., "$123.45" or "123.45")
    (re.compile(r"\$?\d+\.\d{2}\b"), "<AMOUNT>", "amount"),
    # Known product ID patterns (10 uppercase alphanum)
    (re.compile(r"\b[A-Z0-9]{10}\b"), "<PRODUCT_ID>", "product_id"),
    # Numeric values after = or : or space (context-sensitive)
    (re.compile(r"(?<=[=:\s])\d+(?:\.\d+)?(?=[\s,;\]\})]|$)"), "<NUMBER>", "number"),
]


@dataclass
class ExtractedTemplate:
    """Result of normalizing a log message into a template."""

    template: str
    fields: list[LogFieldProfile]


def normalize_log_message(message: str) -> ExtractedTemplate:
    """Normalize a log message by replacing variable parts with placeholders.

    Returns the template string and a list of LogFieldProfiles describing
    how to fill each placeholder type.
    """
    template = message
    field_types_seen: dict[str, str] = {}  # placeholder -> strategy

    for pattern, placeholder, strategy in _NORMALIZERS:
        if pattern.search(template):
            template = pattern.sub(placeholder, template)
            field_types_seen[placeholder] = strategy

    fields = []
    for placeholder, strategy in field_types_seen.items():
        fields.append(LogFieldProfile(
            placeholder=placeholder,
            strategy=strategy,
        ))

    return ExtractedTemplate(template=template, fields=fields)


def extract_templates_from_messages(
    messages: list[str],
    services: list[str],
    severities: list[str],
) -> dict[str, dict]:
    """Extract log templates from a list of raw log messages.

    Groups messages by (service, severity, normalized_template).

    Returns a dict mapping template_key -> {
        "template": str,
        "service": str,
        "severity": str,
        "fields": list[LogFieldProfile],
        "count": int,
        "raw_samples": list[str],  # up to 5 raw examples
    }
    """
    groups: dict[str, dict] = {}

    for msg, svc, sev in zip(messages, services, severities):
        extracted = normalize_log_message(msg)
        key = f"{svc}|{sev}|{extracted.template}"

        if key not in groups:
            groups[key] = {
                "template": extracted.template,
                "service": svc,
                "severity": sev,
                "fields": extracted.fields,
                "count": 0,
                "raw_samples": [],
            }

        groups[key]["count"] += 1
        if len(groups[key]["raw_samples"]) < 5:
            groups[key]["raw_samples"].append(msg)

    return groups
