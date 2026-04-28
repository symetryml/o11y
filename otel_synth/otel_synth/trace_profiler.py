# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Phase 2T: Profile traces — template discovery, error variants, delta computation."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

from otel_synth.config import SeriesStats
from otel_synth.trace_config import (
    AttributeProfile,
    ChildEdge,
    LogTemplateProfile,
    SpanProfile,
    TraceProfileMetadata,
    TraceRegimeProfile,
    TraceTemplate,
)
from otel_synth.models.span_profile import (
    categorize_attribute,
    compute_duration_stats,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Span tree construction from flat DataFrame
# ---------------------------------------------------------------------------


def _build_span_trees(traces_df: pd.DataFrame) -> dict[str, list[dict]]:
    """Build per-trace span trees from a flat traces DataFrame.

    Returns dict mapping trace_id -> list of span dicts (tree-enriched).
    Each span dict has 'children' key with ordered child dicts.
    """
    trees: dict[str, list[dict]] = {}

    for trace_id, group in traces_df.groupby("trace_id"):
        spans_by_id: dict[str, dict] = {}
        for _, row in group.iterrows():
            span = {
                "span_id": row["span_id"],
                "parent_span_id": row.get("parent_span_id", ""),
                "operation_name": row["operation_name"],
                "service_name": row["service_name"],
                "start_time": row["start_time"],
                "duration_us": row["duration_us"],
                "status_code": row.get("status_code", "OK"),
                "tags_json": row.get("tags_json", "{}"),
                "logs_json": row.get("logs_json", "[]"),
                "children": [],
            }
            spans_by_id[span["span_id"]] = span

        # Build tree
        roots = []
        for span in spans_by_id.values():
            parent_id = str(span["parent_span_id"]).strip()
            if parent_id and parent_id in spans_by_id:
                spans_by_id[parent_id]["children"].append(span)
            else:
                roots.append(span)

        # Sort children by start_time
        for span in spans_by_id.values():
            span["children"].sort(key=lambda s: s["start_time"])

        trees[str(trace_id)] = roots

    return trees


# ---------------------------------------------------------------------------
# Structural signature
# ---------------------------------------------------------------------------


def _structural_signature(span: dict) -> str:
    """Create a structural signature for a span tree via DFS.

    Detects repeating groups among siblings and normalizes them.
    Returns a hash string.
    """
    parts = _signature_parts(span)
    sig_str = json.dumps(parts, sort_keys=True)
    return hashlib.sha256(sig_str.encode()).hexdigest()[:16]


def _signature_parts(span: dict) -> list:
    """Recursive DFS to build signature tuple list."""
    node = [span["service_name"], span["operation_name"]]
    child_sigs = []
    for child in span.get("children", []):
        child_sigs.append(_signature_parts(child))

    # Detect repeating subsequences among children
    child_sigs = _collapse_repeats(child_sigs)
    if child_sigs:
        node.append(child_sigs)
    return node


def _collapse_repeats(items: list) -> list:
    """Detect and collapse repeating subsequences.

    E.g., [A, B, A, B, A, B] with k=2 → [REPEAT([A, B], 3)]
    """
    if len(items) <= 2:
        return items

    # Try group sizes from 1 up to half the list
    best_k = 0
    best_count = 0
    for k in range(1, len(items) // 2 + 1):
        # Check if items[:k] repeats
        count = 1
        for i in range(k, len(items) - k + 1, k):
            if items[i:i + k] == items[:k]:
                count += 1
            else:
                break
        if count >= 2 and count * k > best_count * best_k:
            best_k = k
            best_count = count

    if best_count >= 2:
        group = items[:best_k]
        remainder = items[best_count * best_k:]
        collapsed = [["REPEAT", group, best_count]]
        if remainder:
            collapsed.extend(_collapse_repeats(remainder))
        return collapsed

    return items


# ---------------------------------------------------------------------------
# Error signature
# ---------------------------------------------------------------------------

_ERROR_CODES = {"ERROR", "2", "STATUS_CODE_ERROR"}


def _is_error_span(span: dict) -> bool:
    """Check if a span has error status."""
    status = str(span.get("status_code", "")).upper().strip()
    if status in _ERROR_CODES:
        return True
    # Also check tags
    tags = _parse_tags(span.get("tags_json", "{}"))
    return tags.get("otel.status_code", "").upper() in _ERROR_CODES


def _parse_tags(tags_json: str) -> dict[str, str]:
    """Parse tags JSON string to dict."""
    if not tags_json or tags_json == "{}":
        return {}
    try:
        return json.loads(tags_json)
    except (json.JSONDecodeError, TypeError):
        return {}


def _error_positions(span: dict, path: str = "0") -> frozenset[str]:
    """Collect error span positions as frozenset of path strings."""
    positions: set[str] = set()
    if _is_error_span(span):
        positions.add(f"{path}:{span['service_name']}:{span['operation_name']}")
    for i, child in enumerate(span.get("children", [])):
        positions |= _error_positions(child, f"{path}.{i}")
    return frozenset(positions)


# ---------------------------------------------------------------------------
# Overlap detection (sequential vs parallel)
# ---------------------------------------------------------------------------


def _detect_relation(
    spans_a: list[dict], spans_b: list[dict]
) -> str:
    """Detect if two sibling span groups are sequential or parallel.

    Compares start/end times across multiple trace instances.
    Returns "parallel" if median overlap fraction > 0.3, else "sequential".
    """
    if not spans_a or not spans_b:
        return "sequential"

    overlap_fractions = []
    for sa, sb in zip(spans_a, spans_b):
        start_a = sa["start_time"]
        end_a = start_a + sa["duration_us"]
        start_b = sb["start_time"]
        end_b = start_b + sb["duration_us"]

        overlap = max(0, min(end_a, end_b) - max(start_a, start_b))
        span_dur = min(end_a - start_a, end_b - start_b)
        if span_dur > 0:
            overlap_fractions.append(overlap / span_dur)

    if not overlap_fractions:
        return "sequential"

    return "parallel" if np.median(overlap_fractions) > 0.3 else "sequential"


# ---------------------------------------------------------------------------
# Template discovery: group traces and build SpanProfile trees
# ---------------------------------------------------------------------------


def _discover_templates(
    trees: dict[str, list[dict]],
) -> dict[str, dict]:
    """Discover trace templates by structural + error signatures.

    Returns dict mapping combined_signature -> {
        "structural_sig": str,
        "error_sig": frozenset,
        "trace_ids": list[str],
        "roots": list[dict],  # one root span dict per trace instance
    }
    """
    groups: dict[str, dict] = {}

    for trace_id, roots in trees.items():
        if not roots:
            continue
        # Use first root (most traces have one root)
        root = roots[0]
        struct_sig = _structural_signature(root)
        err_sig = _error_positions(root)
        combined = f"{struct_sig}|{hash(err_sig)}"

        if combined not in groups:
            groups[combined] = {
                "structural_sig": struct_sig,
                "error_sig": err_sig,
                "trace_ids": [],
                "roots": [],
            }
        groups[combined]["trace_ids"].append(trace_id)
        groups[combined]["roots"].append(root)

    return groups


def _profile_span_position(
    spans: list[dict],
    all_trace_roots: list[list[dict]],
) -> SpanProfile:
    """Profile a single span position across multiple trace instances.

    spans: list of span dicts at this position from different traces.
    """
    durations = np.array([s["duration_us"] for s in spans], dtype=float)
    dur_stats = compute_duration_stats(durations)

    # Error rate
    n_errors = sum(1 for s in spans if _is_error_span(s))
    error_rate = n_errors / len(spans) if spans else 0.0

    # Status messages for errors
    status_messages = []
    for s in spans:
        if _is_error_span(s):
            tags = _parse_tags(s.get("tags_json", "{}"))
            msg = tags.get("otel.status_description", "")
            if msg and msg not in status_messages:
                status_messages.append(msg)

    # Attributes
    attr_values: dict[str, list[str]] = {}
    for s in spans:
        tags = _parse_tags(s.get("tags_json", "{}"))
        for k, v in tags.items():
            if k.startswith("otel."):
                continue  # skip otel internal attributes
            attr_values.setdefault(k, []).append(str(v))

    attr_profiles = [categorize_attribute(k, vs) for k, vs in attr_values.items()]

    # Span kind from tags
    span_kind = "SPAN_KIND_SERVER"
    for s in spans[:1]:
        tags = _parse_tags(s.get("tags_json", "{}"))
        kind = tags.get("span.kind", tags.get("otel.span_kind", ""))
        if kind:
            if not kind.startswith("SPAN_KIND_"):
                kind = f"SPAN_KIND_{kind.upper()}"
            span_kind = kind

    # Children — profile recursively
    children_edges = _profile_children(spans)

    return SpanProfile(
        service_name=spans[0]["service_name"],
        operation_name=spans[0]["operation_name"],
        span_kind=span_kind,
        duration_us=dur_stats,
        error_rate=error_rate,
        status_message_catalog=status_messages[:20],
        attributes=attr_profiles,
        children=children_edges,
    )


def _profile_children(parent_spans: list[dict]) -> list[ChildEdge]:
    """Profile children of a set of parent span instances.

    Detects repeatable children and groups, and determines sequential vs parallel.
    """
    if not parent_spans:
        return []

    # Collect children per parent
    children_per_parent: list[list[dict]] = [
        s.get("children", []) for s in parent_spans
    ]

    if not any(children_per_parent):
        return []

    # Analyze the canonical child sequence from the first parent with children
    canonical = None
    for children in children_per_parent:
        if children:
            canonical = children
            break
    if canonical is None:
        return []

    # Build child signature sequence for each parent
    child_sig_sequences: list[list[str]] = []
    for children in children_per_parent:
        seq = [f"{c['service_name']}:{c['operation_name']}" for c in children]
        child_sig_sequences.append(seq)

    # Detect repeating groups in the canonical sequence
    canonical_sigs = [f"{c['service_name']}:{c['operation_name']}" for c in canonical]
    repeat_info = _detect_repeating_groups(canonical_sigs)

    edges: list[ChildEdge] = []

    if repeat_info:
        # Process repeating groups
        for group_sigs, group_start, group_size in repeat_info:
            # Determine if this is a repeating group or single repeat
            is_group = group_size > 1

            # Collect repeat counts across all parents
            repeat_counts = []
            for seq in child_sig_sequences:
                count = _count_group_repeats(seq, group_sigs)
                repeat_counts.append(count)

            repeat_arr = np.array(repeat_counts, dtype=float)
            rcs = compute_duration_stats(repeat_arr) if np.std(repeat_arr) > 0 or np.mean(repeat_arr) > 1 else None
            is_repeatable = rcs is not None and (np.std(repeat_arr) > 0 or np.mean(repeat_arr) > 1)

            if is_group:
                # Collect child spans for each position in the group
                group_profiles = []
                for gi in range(group_size):
                    position_spans = []
                    for parent_children in children_per_parent:
                        # Find all instances of this group position
                        for j in range(gi, len(parent_children), group_size):
                            sig = f"{parent_children[j]['service_name']}:{parent_children[j]['operation_name']}"
                            if sig == group_sigs[gi]:
                                position_spans.append(parent_children[j])
                    if position_spans:
                        group_profiles.append(
                            _profile_span_position(position_spans, [])
                        )

                # Detect relation within group
                relation = "sequential"
                if group_size >= 2:
                    first_spans = [pc[group_start] for pc in children_per_parent if len(pc) > group_start]
                    second_spans = [pc[group_start + 1] for pc in children_per_parent if len(pc) > group_start + 1]
                    relation = _detect_relation(first_spans, second_spans)

                edges.append(ChildEdge(
                    child=group_profiles[0] if group_profiles else None,
                    relation=relation,
                    is_repeatable=is_repeatable,
                    repeat_count_stats=rcs,
                    is_group=is_group,
                    group_children=group_profiles if is_group else None,
                ))
            else:
                # Single repeatable child
                position_spans = []
                for parent_children in children_per_parent:
                    for c in parent_children:
                        sig = f"{c['service_name']}:{c['operation_name']}"
                        if sig == group_sigs[0]:
                            position_spans.append(c)
                if position_spans:
                    child_prof = _profile_span_position(position_spans, [])
                    edges.append(ChildEdge(
                        child=child_prof,
                        relation="sequential",
                        is_repeatable=is_repeatable,
                        repeat_count_stats=rcs,
                    ))
    else:
        # No repeating groups — process children positionally
        n_children = len(canonical)
        for ci in range(n_children):
            position_spans = [
                pc[ci] for pc in children_per_parent if ci < len(pc)
            ]
            if not position_spans:
                continue

            child_prof = _profile_span_position(position_spans, [])

            # Detect relation with previous sibling
            relation = "sequential"
            if ci > 0:
                prev_spans = [pc[ci - 1] for pc in children_per_parent if ci - 1 < len(pc) and ci < len(pc)]
                curr_spans = [pc[ci] for pc in children_per_parent if ci < len(pc) and ci - 1 < len(pc)]
                relation = _detect_relation(prev_spans, curr_spans)

            edges.append(ChildEdge(
                child=child_prof,
                relation=relation,
            ))

    return edges


def _detect_repeating_groups(sigs: list[str]) -> list[tuple[list[str], int, int]]:
    """Detect repeating subsequences in a list of child signatures.

    Returns list of (group_sigs, start_index, group_size) tuples.
    Returns empty list if no significant repetition found.
    """
    if len(sigs) <= 2:
        return []

    # Try group sizes from 1 up to half the list
    for k in range(1, len(sigs) // 2 + 1):
        group = sigs[:k]
        count = 0
        for i in range(0, len(sigs) - k + 1, k):
            if sigs[i:i + k] == group:
                count += 1
            else:
                break
        # Need at least 3 repeats or cover >50% of children to be significant
        if count >= 3 or (count >= 2 and count * k >= len(sigs) * 0.5):
            return [(group, 0, k)]

    return []


def _count_group_repeats(seq: list[str], group_sigs: list[str]) -> int:
    """Count how many times group_sigs repeats in seq."""
    k = len(group_sigs)
    if k == 0:
        return 0
    count = 0
    for i in range(0, len(seq) - k + 1, k):
        if seq[i:i + k] == group_sigs:
            count += 1
        else:
            break
    return count


# ---------------------------------------------------------------------------
# Gap fraction computation
# ---------------------------------------------------------------------------


def _compute_gap_fractions(spans: list[dict]) -> np.ndarray:
    """Compute gap fractions: (parent_dur - sum(child_durs)) / parent_dur."""
    fractions = []
    for span in spans:
        parent_dur = span["duration_us"]
        if parent_dur <= 0:
            fractions.append(0.0)
            continue
        children = span.get("children", [])
        child_sum = sum(c["duration_us"] for c in children)
        gap = max(parent_dur - child_sum, 0)
        fractions.append(gap / parent_dur)
    return np.array(fractions) if fractions else np.array([0.0])


# ---------------------------------------------------------------------------
# Resource attributes extraction
# ---------------------------------------------------------------------------


def _extract_resource_attributes(spans: list[dict]) -> dict[str, str]:
    """Extract common resource attributes from root spans."""
    if not spans:
        return {}
    # Use tags from first span as representative
    tags = _parse_tags(spans[0].get("tags_json", "{}"))
    resource_keys = [
        "service.version", "service.namespace", "host.name",
        "telemetry.sdk.language", "telemetry.sdk.version",
    ]
    return {k: tags[k] for k in resource_keys if k in tags}


# ---------------------------------------------------------------------------
# Template naming
# ---------------------------------------------------------------------------


def _make_template_name(root: dict, has_errors: bool) -> tuple[str, str]:
    """Create template name and base template name.

    Returns (template_name, base_template_name).
    """
    # Use root service + operation as base
    svc = root["service_name"].replace("-", "_").replace(" ", "_")
    op = root["operation_name"].replace("/", "_").replace(" ", "_").strip("_")
    base = f"{svc}_{op}" if op else svc

    if has_errors:
        return f"{base}_error", base
    return f"{base}_ok", base


# ---------------------------------------------------------------------------
# Public API: profile_traces
# ---------------------------------------------------------------------------


def profile_traces(
    traces_df: pd.DataFrame,
    regime_name: str,
    is_baseline: bool = True,
    source_csv: str = "",
) -> TraceRegimeProfile:
    """Profile traces from a DataFrame into a TraceRegimeProfile.

    Args:
        traces_df: DataFrame with columns: trace_id, span_id, parent_span_id,
                   operation_name, service_name, start_time, duration_us,
                   status_code, tags_json, logs_json
        regime_name: name of this regime
        is_baseline: whether this is the baseline regime
        source_csv: path to source CSV file

    Returns:
        TraceRegimeProfile with all discovered templates
    """
    logger.info(f"Profiling traces for regime '{regime_name}' ({len(traces_df)} spans)")

    # Ensure start_time is numeric (microseconds)
    if traces_df["start_time"].dtype == object:
        traces_df = traces_df.copy()
        traces_df["start_time"] = pd.to_datetime(traces_df["start_time"]).astype(np.int64) // 1000

    # Build span trees
    trees = _build_span_trees(traces_df)
    n_traces = len(trees)
    logger.info(f"Built {n_traces} span trees")

    # Discover templates
    template_groups = _discover_templates(trees)
    logger.info(f"Discovered {len(template_groups)} template groups")

    # Compute request rate
    if n_traces > 0 and "start_time" in traces_df.columns:
        root_times = []
        for roots in trees.values():
            if roots:
                root_times.append(roots[0]["start_time"])
        if len(root_times) >= 2:
            root_times.sort()
            duration_us = root_times[-1] - root_times[0]
            duration_s = duration_us / 1_000_000 if duration_us > 0 else 1.0
            total_request_rate = n_traces / duration_s
        else:
            total_request_rate = 1.0
    else:
        total_request_rate = 1.0

    # Build templates
    trace_templates: dict[str, TraceTemplate] = {}
    template_weights: dict[str, float] = {}

    for combined_sig, group in template_groups.items():
        roots = group["roots"]
        has_errors = bool(group["error_sig"])

        template_name, base_name = _make_template_name(roots[0], has_errors)

        # Avoid duplicate template names
        if template_name in trace_templates:
            template_name = f"{template_name}_{group['structural_sig'][:8]}"

        # Profile root span position
        root_profile = _profile_span_position(roots, [])

        # Gap fractions
        gap_fracs = _compute_gap_fractions(roots)
        gap_stats = compute_duration_stats(gap_fracs)

        # Resource attributes
        resource_attrs = _extract_resource_attributes(roots)

        # Error span positions
        error_positions = None
        if has_errors:
            error_positions = sorted(group["error_sig"])

        weight = len(roots) / n_traces if n_traces > 0 else 0.0

        trace_templates[template_name] = TraceTemplate(
            template_name=template_name,
            base_template_name=base_name,
            signature=group["structural_sig"],
            is_error_variant=has_errors,
            error_span_positions=error_positions,
            root_span=root_profile,
            weight=weight,
            n_instances=len(roots),
            gap_fraction_stats=gap_stats,
            resource_attributes=resource_attrs,
        )
        template_weights[template_name] = weight

    # Compute duration
    duration_s = 0.0
    if not traces_df.empty:
        ts_min = traces_df["start_time"].min()
        ts_max = traces_df["start_time"].max()
        duration_s = (ts_max - ts_min) / 1_000_000

    metadata = TraceProfileMetadata(
        regime_name=regime_name,
        is_baseline=is_baseline,
        duration_seconds=duration_s,
        source_traces_csv=source_csv,
        n_traces=n_traces,
        n_spans=len(traces_df),
        n_templates=len(trace_templates),
    )

    profile = TraceRegimeProfile(
        metadata=metadata,
        trace_templates=trace_templates,
        total_request_rate=total_request_rate,
        template_weights=template_weights,
    )

    logger.info(
        f"Trace profile: {len(trace_templates)} templates, "
        f"{total_request_rate:.2f} req/s, {duration_s:.0f}s duration"
    )
    return profile


# ---------------------------------------------------------------------------
# Delta computation (baseline vs anomaly)
# ---------------------------------------------------------------------------


def compute_trace_deltas(
    baseline: TraceRegimeProfile,
    anomaly: TraceRegimeProfile,
) -> TraceRegimeProfile:
    """Compute anomaly deltas relative to baseline for trace profiles.

    Matches templates by base_template_name, computes delta weights,
    and per-span duration/error deltas.
    """
    # Request rate delta
    anomaly.delta_request_rate = anomaly.total_request_rate - baseline.total_request_rate

    # Match templates by base name
    baseline_by_base: dict[str, list[str]] = defaultdict(list)
    for tname, tmpl in baseline.trace_templates.items():
        baseline_by_base[tmpl.base_template_name].append(tname)

    anomaly_by_base: dict[str, list[str]] = defaultdict(list)
    for tname, tmpl in anomaly.trace_templates.items():
        anomaly_by_base[tmpl.base_template_name].append(tname)

    # Weight deltas
    delta_weights: dict[str, float] = {}
    for tname, tmpl in anomaly.trace_templates.items():
        baseline_weight = 0.0
        # Find matching baseline template
        for bt_name in baseline_by_base.get(tmpl.base_template_name, []):
            bt = baseline.trace_templates[bt_name]
            if bt.is_error_variant == tmpl.is_error_variant:
                baseline_weight = bt.weight
                # Compute per-span deltas
                _compute_span_deltas(bt.root_span, tmpl.root_span)
                break
        delta_weights[tname] = tmpl.weight - baseline_weight

    anomaly.delta_template_weights = delta_weights

    return anomaly


def _compute_span_deltas(baseline_span: SpanProfile, anomaly_span: SpanProfile) -> None:
    """Compute per-span deltas recursively."""
    anomaly_span.delta_duration_mean = (
        anomaly_span.duration_us.mean - baseline_span.duration_us.mean
    )
    anomaly_span.delta_duration_std = (
        anomaly_span.duration_us.std - baseline_span.duration_us.std
    )
    anomaly_span.delta_error_rate = (
        anomaly_span.error_rate - baseline_span.error_rate
    )

    # Recurse into children (match by position)
    for b_edge, a_edge in zip(baseline_span.children, anomaly_span.children):
        if b_edge.child and a_edge.child:
            _compute_span_deltas(b_edge.child, a_edge.child)
        if b_edge.group_children and a_edge.group_children:
            for bc, ac in zip(b_edge.group_children, a_edge.group_children):
                _compute_span_deltas(bc, ac)


# ---------------------------------------------------------------------------
# Public API: profile_all_traces
# ---------------------------------------------------------------------------


def _profile_traces_from_csv(
    csv_path: str,
    regime_name: str,
    is_baseline: bool,
) -> TraceRegimeProfile:
    """Load a trace CSV and profile it. Top-level function for multiprocessing."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    traces_df = pd.read_csv(csv_path)
    return profile_traces(
        traces_df, regime_name, is_baseline=is_baseline, source_csv=csv_path
    )


def profile_all_traces(
    regimes_path: str | Path,
    output_dir: str | Path,
    workers: int = 0,
) -> dict[str, TraceRegimeProfile]:
    """Profile traces for all regimes defined in regimes.json.

    Args:
        regimes_path: path to regimes.json
        output_dir: directory to save .trace.profile.json files
        workers: number of parallel worker processes (0 = sequential)

    Returns:
        dict mapping regime_name -> TraceRegimeProfile
    """
    regimes_path = Path(regimes_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(regimes_path) as f:
        regimes_config = json.load(f)

    # Resolve all trace CSV paths
    def _resolve_trace_csv(regime_name, regime_value):
        if isinstance(regime_value, str):
            logger.info(f"Skipping '{regime_name}' — no trace CSV (metrics-only)")
            return None
        elif isinstance(regime_value, dict):
            traces_csv = regime_value.get("traces")
            if not traces_csv:
                logger.info(f"Skipping '{regime_name}' — no trace CSV")
                return None
        else:
            return None

        csv_path = Path(traces_csv)
        if not csv_path.is_absolute():
            csv_path = regimes_path.parent / csv_path

        if not csv_path.exists():
            logger.warning(f"Trace CSV not found for '{regime_name}': {csv_path}")
            return None

        return str(csv_path)

    profiles: dict[str, TraceRegimeProfile] = {}
    baseline_profile: TraceRegimeProfile | None = None

    # Profile baseline first (always sequential — needed for deltas)
    baseline_csv = _resolve_trace_csv("baseline", regimes_config.get("baseline", ""))
    if baseline_csv:
        logger.info(f"Loading baseline trace CSV: {baseline_csv}")
        baseline_profile = _profile_traces_from_csv(baseline_csv, "baseline", True)
        out_path = output_dir / "baseline.trace.profile.json"
        baseline_profile.save(out_path)
        logger.info(f"Saved baseline trace profile to {out_path}")
        profiles["baseline"] = baseline_profile

    # Collect anomaly regimes
    anomaly_regimes = {}
    for regime_name, regime_value in regimes_config.items():
        if regime_name == "baseline":
            continue
        csv_path = _resolve_trace_csv(regime_name, regime_value)
        if csv_path:
            anomaly_regimes[regime_name] = csv_path

    if not anomaly_regimes:
        return profiles

    if workers > 0:
        n_workers = min(workers, len(anomaly_regimes))
        logger.info(f"Profiling {len(anomaly_regimes)} anomaly trace regimes with {n_workers} workers")

        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            future_to_name = {
                executor.submit(
                    _profile_traces_from_csv, csv_path, regime_name, False
                ): regime_name
                for regime_name, csv_path in anomaly_regimes.items()
            }
            for future in as_completed(future_to_name):
                regime_name = future_to_name[future]
                try:
                    profile = future.result()
                except Exception:
                    logger.exception(f"Failed to profile trace regime '{regime_name}'")
                    continue

                if baseline_profile is not None:
                    profile = compute_trace_deltas(baseline_profile, profile)

                out_path = output_dir / f"{regime_name}.trace.profile.json"
                profile.save(out_path)
                logger.info(f"Saved trace profile to {out_path}")
                profiles[regime_name] = profile
    else:
        for regime_name, csv_path in anomaly_regimes.items():
            logger.info(f"Loading trace CSV for '{regime_name}': {csv_path}")
            profile = _profile_traces_from_csv(csv_path, regime_name, False)

            if baseline_profile is not None:
                profile = compute_trace_deltas(baseline_profile, profile)

            out_path = output_dir / f"{regime_name}.trace.profile.json"
            profile.save(out_path)
            logger.info(f"Saved trace profile to {out_path}")
            profiles[regime_name] = profile

    return profiles
