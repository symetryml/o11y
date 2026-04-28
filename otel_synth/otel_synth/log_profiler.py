# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Phase 2L: Profile logs — template extraction, span association, emission rates."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

from otel_synth.trace_config import (
    LogFieldProfile,
    LogRegimeProfile,
    LogTemplateProfile,
    TraceProfileMetadata,
)
from otel_synth.models.log_template import (
    normalize_log_message,
    extract_templates_from_messages,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Span association
# ---------------------------------------------------------------------------


def _associate_logs_with_spans(
    logs_df: pd.DataFrame,
    traces_df: pd.DataFrame | None,
) -> pd.DataFrame:
    """Join logs with traces on (trace_id, span_id) to identify which span emits each log.

    Returns logs_df with additional columns: span_operation, span_service.
    """
    if traces_df is None or traces_df.empty:
        logs_df = logs_df.copy()
        logs_df["span_operation"] = ""
        logs_df["span_service"] = logs_df.get("service", "")
        return logs_df

    # Build span lookup: (trace_id, span_id) -> (operation_name, service_name)
    span_lookup = {}
    for _, row in traces_df.iterrows():
        key = (str(row["trace_id"]), str(row["span_id"]))
        span_lookup[key] = (row["operation_name"], row["service_name"])

    span_ops = []
    span_svcs = []
    for _, row in logs_df.iterrows():
        trace_id = str(row.get("trace_id", ""))
        span_id = str(row.get("span_id", ""))
        key = (trace_id, span_id)
        if key in span_lookup:
            op, svc = span_lookup[key]
            span_ops.append(op)
            span_svcs.append(svc)
        else:
            span_ops.append("")
            span_svcs.append(row.get("service", ""))

    logs_df = logs_df.copy()
    logs_df["span_operation"] = span_ops
    logs_df["span_service"] = span_svcs
    return logs_df


# ---------------------------------------------------------------------------
# Emission rate computation
# ---------------------------------------------------------------------------


def _compute_emission_rates(
    log_template_groups: dict[str, dict],
    traces_df: pd.DataFrame | None,
) -> dict[str, float]:
    """Compute emission rates: P(log emitted | matching span occurs).

    Returns dict mapping template_key -> emission_rate.
    """
    if traces_df is None or traces_df.empty:
        return {key: 1.0 for key in log_template_groups}

    # Count spans per (service, operation)
    span_counts: dict[tuple[str, str], int] = defaultdict(int)
    for _, row in traces_df.iterrows():
        key = (str(row["service_name"]), str(row["operation_name"]))
        span_counts[key] += 1

    rates = {}
    for template_key, group in log_template_groups.items():
        svc = group.get("associated_span_service", group["service"])
        op = group.get("associated_span_operation", "")
        n_logs = group["count"]

        if op and (svc, op) in span_counts:
            n_spans = span_counts[(svc, op)]
            rates[template_key] = min(n_logs / max(n_spans, 1), 1.0)
        else:
            # No span association — treat as standalone log
            rates[template_key] = 1.0

    return rates


# ---------------------------------------------------------------------------
# Public API: profile_logs
# ---------------------------------------------------------------------------


def profile_logs(
    logs_df: pd.DataFrame,
    regime_name: str,
    is_baseline: bool = True,
    traces_df: pd.DataFrame | None = None,
    source_csv: str = "",
) -> dict[str, LogTemplateProfile]:
    """Profile logs from a DataFrame into LogTemplateProfiles.

    Args:
        logs_df: DataFrame with columns: timestamp, service, severity, message,
                 trace_id, span_id
        regime_name: name of this regime
        is_baseline: whether this is the baseline regime
        traces_df: optional traces DataFrame for span association
        source_csv: path to source CSV file

    Returns:
        dict mapping template_key -> LogTemplateProfile
    """
    logger.info(f"Profiling logs for regime '{regime_name}' ({len(logs_df)} log records)")

    # Associate logs with spans if traces available
    if traces_df is not None and not traces_df.empty:
        logs_df = _associate_logs_with_spans(logs_df, traces_df)

    # Extract templates
    messages = logs_df["message"].astype(str).tolist()
    services = logs_df["service"].astype(str).tolist()
    severities = logs_df["severity"].astype(str).tolist()

    template_groups = extract_templates_from_messages(messages, services, severities)
    logger.info(f"Extracted {len(template_groups)} log templates")

    # Add span association info
    if "span_operation" in logs_df.columns:
        for key, group in template_groups.items():
            svc = group["service"]
            # Find the most common span operation for logs matching this template
            matching_indices = []
            for i, (msg, s, sv) in enumerate(zip(messages, services, severities)):
                extracted = normalize_log_message(msg)
                candidate_key = f"{s}|{sv}|{extracted.template}"
                if candidate_key == key:
                    matching_indices.append(i)

            if matching_indices:
                ops = [logs_df.iloc[i].get("span_operation", "") for i in matching_indices[:200]]
                ops = [o for o in ops if o]
                if ops:
                    from collections import Counter
                    most_common_op = Counter(ops).most_common(1)[0][0]
                    group["associated_span_operation"] = most_common_op
                    group["associated_span_service"] = svc
                else:
                    group["associated_span_operation"] = ""
                    group["associated_span_service"] = svc
            else:
                group["associated_span_operation"] = ""
                group["associated_span_service"] = svc

    # Compute emission rates
    emission_rates = _compute_emission_rates(template_groups, traces_df)

    # Build LogTemplateProfile objects
    log_profiles: dict[str, LogTemplateProfile] = {}
    for key, group in template_groups.items():
        template_key = key
        log_profiles[template_key] = LogTemplateProfile(
            template_key=template_key,
            service_name=group["service"],
            severity=group["severity"],
            body_template=group["template"],
            body_fields=group["fields"],
            emission_rate=emission_rates.get(key, 1.0),
            associated_span_operation=group.get("associated_span_operation", ""),
            associated_span_service=group.get("associated_span_service", group["service"]),
        )

    logger.info(f"Log profile: {len(log_profiles)} templates")
    return log_profiles


# ---------------------------------------------------------------------------
# Delta computation
# ---------------------------------------------------------------------------


def compute_log_deltas(
    baseline_logs: dict[str, LogTemplateProfile],
    anomaly_logs: dict[str, LogTemplateProfile],
) -> dict[str, LogTemplateProfile]:
    """Compute anomaly deltas for log templates relative to baseline.

    Updates anomaly_logs in place with delta_emission_rate and existence.
    """
    for key, lt in anomaly_logs.items():
        if key in baseline_logs:
            baseline_lt = baseline_logs[key]
            lt.delta_emission_rate = lt.emission_rate - baseline_lt.emission_rate
            lt.existence = "both"
        else:
            lt.existence = "emergent"
            lt.delta_emission_rate = lt.emission_rate

    # Mark disappeared templates
    for key in baseline_logs:
        if key not in anomaly_logs:
            # Create a placeholder for disappeared template
            bl = baseline_logs[key]
            anomaly_logs[key] = LogTemplateProfile(
                template_key=key,
                service_name=bl.service_name,
                severity=bl.severity,
                body_template=bl.body_template,
                body_fields=bl.body_fields,
                emission_rate=0.0,
                associated_span_operation=bl.associated_span_operation,
                associated_span_service=bl.associated_span_service,
                delta_emission_rate=-bl.emission_rate,
                existence="disappeared",
            )

    return anomaly_logs


# ---------------------------------------------------------------------------
# Public API: profile_all_logs
# ---------------------------------------------------------------------------


def _profile_logs_from_csv(
    logs_csv: str,
    traces_csv: str | None,
    regime_name: str,
    is_baseline: bool,
) -> dict[str, LogTemplateProfile]:
    """Load log (and optional trace) CSV and profile. Top-level function for multiprocessing."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logs_df = pd.read_csv(logs_csv)
    traces_df = None
    if traces_csv:
        traces_path = Path(traces_csv)
        if traces_path.exists():
            traces_df = pd.read_csv(traces_path)
    return profile_logs(
        logs_df, regime_name, is_baseline=is_baseline,
        traces_df=traces_df, source_csv=logs_csv,
    )


def profile_all_logs(
    regimes_path: str | Path,
    output_dir: str | Path,
    workers: int = 0,
) -> dict[str, dict[str, LogTemplateProfile]]:
    """Profile logs for all regimes defined in regimes.json.

    Args:
        regimes_path: path to regimes.json
        output_dir: directory to save .log.profile.json files
        workers: number of parallel worker processes (0 = sequential)

    Returns:
        dict mapping regime_name -> dict[template_key -> LogTemplateProfile]
    """
    regimes_path = Path(regimes_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(regimes_path) as f:
        regimes_config = json.load(f)

    def _resolve_log_csvs(regime_name, regime_value):
        """Returns (logs_csv, traces_csv) or None if no log CSV."""
        if isinstance(regime_value, str):
            logger.info(f"Skipping '{regime_name}' — no log CSV (metrics-only)")
            return None
        elif isinstance(regime_value, dict):
            logs_csv = regime_value.get("logs")
            if not logs_csv:
                logger.info(f"Skipping '{regime_name}' — no log CSV")
                return None
        else:
            return None

        csv_path = Path(logs_csv)
        if not csv_path.is_absolute():
            csv_path = regimes_path.parent / csv_path

        if not csv_path.exists():
            logger.warning(f"Log CSV not found for '{regime_name}': {csv_path}")
            return None

        # Resolve optional traces CSV for span association
        traces_csv_resolved = None
        if isinstance(regime_value, dict) and "traces" in regime_value:
            traces_path = Path(regime_value["traces"])
            if not traces_path.is_absolute():
                traces_path = regimes_path.parent / traces_path
            if traces_path.exists():
                traces_csv_resolved = str(traces_path)

        return str(csv_path), traces_csv_resolved

    def _save_log_profiles(regime_name: str, log_profiles: dict[str, LogTemplateProfile]):
        """Save log profiles as a standalone .log.profile.json file."""
        log_regime = LogRegimeProfile(
            metadata=TraceProfileMetadata(n_log_templates=len(log_profiles)),
            log_templates=log_profiles,
        )
        out_path = output_dir / f"{regime_name}.log.profile.json"
        log_regime.save(out_path)
        logger.info(f"Saved log profile to {out_path}")

    all_profiles: dict[str, dict[str, LogTemplateProfile]] = {}
    baseline_logs: dict[str, LogTemplateProfile] | None = None

    # Profile baseline first (always sequential — needed for deltas)
    resolved = _resolve_log_csvs("baseline", regimes_config.get("baseline", ""))
    if resolved:
        logs_csv, traces_csv = resolved
        logger.info(f"Loading baseline log CSV: {logs_csv}")
        baseline_logs = _profile_logs_from_csv(logs_csv, traces_csv, "baseline", True)
        all_profiles["baseline"] = baseline_logs
        _save_log_profiles("baseline", baseline_logs)
        logger.info(f"Profiled {len(baseline_logs)} log templates for 'baseline'")

    # Collect anomaly regimes
    anomaly_regimes: dict[str, tuple[str, str | None]] = {}
    for regime_name, regime_value in regimes_config.items():
        if regime_name == "baseline":
            continue
        resolved = _resolve_log_csvs(regime_name, regime_value)
        if resolved:
            anomaly_regimes[regime_name] = resolved

    if not anomaly_regimes:
        return all_profiles

    if workers > 0:
        n_workers = min(workers, len(anomaly_regimes))
        logger.info(f"Profiling {len(anomaly_regimes)} anomaly log regimes with {n_workers} workers")

        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            future_to_name = {
                executor.submit(
                    _profile_logs_from_csv, logs_csv, traces_csv, regime_name, False
                ): regime_name
                for regime_name, (logs_csv, traces_csv) in anomaly_regimes.items()
            }
            for future in as_completed(future_to_name):
                regime_name = future_to_name[future]
                try:
                    log_profiles = future.result()
                except Exception:
                    logger.exception(f"Failed to profile log regime '{regime_name}'")
                    continue

                if baseline_logs is not None:
                    log_profiles = compute_log_deltas(baseline_logs, log_profiles)

                all_profiles[regime_name] = log_profiles
                _save_log_profiles(regime_name, log_profiles)
                logger.info(f"Profiled {len(log_profiles)} log templates for '{regime_name}'")
    else:
        for regime_name, (logs_csv, traces_csv) in anomaly_regimes.items():
            logger.info(f"Loading log CSV for '{regime_name}': {logs_csv}")
            log_profiles = _profile_logs_from_csv(logs_csv, traces_csv, regime_name, False)

            if baseline_logs is not None:
                log_profiles = compute_log_deltas(baseline_logs, log_profiles)

            all_profiles[regime_name] = log_profiles
            _save_log_profiles(regime_name, log_profiles)
            logger.info(f"Profiled {len(log_profiles)} log templates for '{regime_name}'")

    return all_profiles
