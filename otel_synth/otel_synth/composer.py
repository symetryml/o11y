"""Phase 3: Compose multi-segment scenarios with regime mixing.

Supports two-tier generation:
  Tier 1: Generate traces → derive trace-correlated metrics
  Tier 2: Generate infrastructure metrics from AR(1) pipeline
  Tier 3: Generate logs correlated to Tier 1 traces
"""

from __future__ import annotations

import copy
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from otel_synth.config import RegimeProfile, SeriesProfile, HistogramFamilyProfile
from otel_synth.generator import generate_from_profile
from otel_synth.trace_config import TraceRegimeProfile
from otel_synth.trace_generator import (
    GeneratedSpan,
    GeneratedLog,
    generate_traces,
    derive_trace_metrics,
)
from otel_synth.otlp_writer import (
    write_traces_otlp,
    write_logs_otlp,
    write_traces_csv,
    write_logs_csv,
)

logger = logging.getLogger(__name__)


def compose(
    scenario_path: str | Path,
    seed: int | None = None,
) -> tuple[pd.DataFrame, Path]:
    """Compose a multi-segment scenario from a YAML config.

    Supports two-tier generation when trace profiles are available:
      Tier 1: Generate traces → derive trace-correlated metrics
      Tier 2: Generate infrastructure metrics from AR(1) pipeline
      Tier 3: Generate logs correlated to Tier 1 traces

    Returns (combined metrics DataFrame, output path).
    """
    scenario_path = Path(scenario_path)

    with open(scenario_path) as f:
        config = yaml.safe_load(f)

    # Output paths
    output_config = config.get("output", "./output/scenario.csv")
    if isinstance(output_config, dict):
        metrics_output = Path(output_config.get("metrics", "./output/metrics.csv"))
        traces_output = Path(output_config.get("traces", "./output/traces.otlp.json"))
        logs_output = Path(output_config.get("logs", "./output/logs.otlp.json"))
    else:
        metrics_output = Path(output_config)
        traces_output = Path(str(output_config).replace(".csv", ".traces.otlp.json"))
        logs_output = Path(str(output_config).replace(".csv", ".logs.otlp.json"))

    # Resolve relative paths
    for attr_name in ("metrics_output", "traces_output", "logs_output"):
        p = locals()[attr_name]
        if not p.is_absolute():
            locals()[attr_name] = scenario_path.parent / p
    metrics_output = scenario_path.parent / metrics_output if not metrics_output.is_absolute() else metrics_output
    traces_output = scenario_path.parent / traces_output if not traces_output.is_absolute() else traces_output
    logs_output = scenario_path.parent / logs_output if not logs_output.is_absolute() else logs_output

    profiles_dir = Path(config.get("profiles_dir", "./profiles/"))
    if not profiles_dir.is_absolute():
        profiles_dir = scenario_path.parent / profiles_dir

    scenario = config["scenario"]
    start_time_str = scenario.get("start_time", "now")
    if start_time_str.lower() == "now":
        start_time = datetime.utcnow()
    else:
        start_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))

    step_seconds = scenario.get("step_seconds", 60)

    # Load all needed profiles
    profile_cache: dict[str, RegimeProfile] = {}
    trace_profile_cache: dict[str, TraceRegimeProfile] = {}

    def get_profile(name: str) -> RegimeProfile:
        if name not in profile_cache:
            path = profiles_dir / f"{name}.profile.json"
            profile_cache[name] = RegimeProfile.load(path)
        return profile_cache[name]

    def get_trace_profile(name: str) -> TraceRegimeProfile | None:
        if name not in trace_profile_cache:
            path = profiles_dir / f"{name}.trace.profile.json"
            if path.exists():
                tp = TraceRegimeProfile.load(path)
            else:
                tp = None

            # Merge standalone log profile if it exists
            log_path = profiles_dir / f"{name}.log.profile.json"
            if log_path.exists():
                from otel_synth.trace_config import LogRegimeProfile
                log_profile = LogRegimeProfile.load(log_path)
                if tp is None:
                    tp = TraceRegimeProfile()
                tp.log_templates = log_profile.log_templates
                tp.metadata.n_log_templates = len(log_profile.log_templates)

            trace_profile_cache[name] = tp
        return trace_profile_cache[name]

    rng = np.random.default_rng(seed)

    segments_dfs: list[pd.DataFrame] = []
    all_spans: list[GeneratedSpan] = []
    all_logs: list[GeneratedLog] = []
    ground_truth_rows: list[dict] = []
    current_time = start_time

    for segment in scenario["segments"]:
        duration_minutes = segment["duration_minutes"]
        n_points = int(duration_minutes * 60 / step_seconds)
        duration_us = int(duration_minutes * 60 * 1_000_000)
        step_us = step_seconds * 1_000_000

        # Determine which regime(s) this segment uses
        if "regime" in segment:
            regime = segment["regime"]
            regime_names = regime if isinstance(regime, list) else [regime]
        elif "regimes" in segment:
            regime_names = segment["regimes"]
        else:
            raise ValueError(f"Segment must have 'regime' or 'regimes': {segment}")

        is_anomaly = not (regime_names == ["baseline"])

        # Build the effective metric profile for this segment
        if regime_names == ["baseline"]:
            effective_profile = get_profile("baseline")
        elif len(regime_names) == 1:
            effective_profile = get_profile(regime_names[0])
        else:
            effective_profile = _compose_anomaly_profiles(
                get_profile("baseline"),
                [get_profile(name) for name in regime_names],
            )

        # --- TIER 1: Trace generation + derived metrics ---
        current_time_us = int(current_time.timestamp() * 1_000_000)
        trace_metric_rows: list[dict] = []

        # Try to get trace profile for the primary regime
        trace_profile = None
        for rn in regime_names:
            trace_profile = get_trace_profile(rn)
            if trace_profile is not None:
                break

        if trace_profile is not None:
            segment_spans, segment_logs = generate_traces(
                trace_profile,
                start_time_us=current_time_us,
                duration_us=duration_us,
                step_seconds=step_seconds,
                rng=rng,
            )
            all_spans.extend(segment_spans)
            all_logs.extend(segment_logs)

            # Derive trace-correlated metrics (Tier 1)
            trace_metric_rows = derive_trace_metrics(
                segment_spans, current_time_us, step_us, n_points
            )
            logger.info(
                f"Tier 1: {len(segment_spans)} spans, {len(segment_logs)} logs, "
                f"{len(trace_metric_rows)} derived metric rows"
            )

        # --- TIER 2: Infrastructure metrics from AR(1) pipeline ---
        segment_df = generate_from_profile(
            effective_profile, current_time, n_points, step_seconds, rng
        )

        # Merge Tier 1 trace-derived metrics with Tier 2 infrastructure metrics
        if trace_metric_rows:
            trace_metrics_df = pd.DataFrame(
                trace_metric_rows,
                columns=["timestamp", "metric", "labels", "value"],
            )
            segment_df = pd.concat([segment_df, trace_metrics_df], ignore_index=True)

        segments_dfs.append(segment_df)

        # Ground truth
        if is_anomaly:
            segment_end = current_time + timedelta(minutes=duration_minutes)
            ground_truth_rows.append({
                "start_time": current_time.isoformat() + "Z",
                "end_time": segment_end.isoformat() + "Z",
                "regimes": ",".join(regime_names),
            })

        current_time += timedelta(minutes=duration_minutes)

    # Combine all metric segments
    combined = pd.concat(segments_dfs, ignore_index=True)
    combined = combined.sort_values(["timestamp", "metric", "labels"]).reset_index(drop=True)

    # Save metrics
    metrics_output.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(metrics_output, index=False)
    logger.info(f"Saved metrics to {metrics_output} ({len(combined)} rows)")

    # Save traces (if any were generated)
    if all_spans:
        write_traces_otlp(all_spans, traces_output)
        logger.info(f"Saved {len(all_spans)} spans to {traces_output}")

    # Save logs (if any were generated)
    if all_logs:
        write_logs_otlp(all_logs, logs_output)
        logger.info(f"Saved {len(all_logs)} logs to {logs_output}")

    # Save ground truth
    gt_config = config.get("ground_truth", {})
    gt_path = gt_config.get("output", str(metrics_output.parent / "ground_truth.csv"))
    if not Path(gt_path).is_absolute():
        gt_path = scenario_path.parent / gt_path
    gt_df = pd.DataFrame(ground_truth_rows, columns=["start_time", "end_time", "regimes"])
    Path(gt_path).parent.mkdir(parents=True, exist_ok=True)
    gt_df.to_csv(gt_path, index=False)
    logger.info(f"Saved ground truth to {gt_path} ({len(gt_df)} anomaly segments)")

    return combined, metrics_output


def _compose_anomaly_profiles(
    baseline: RegimeProfile,
    anomaly_profiles: list[RegimeProfile],
) -> RegimeProfile:
    """Compose multiple anomaly profiles additively on top of baseline.

    Mean shifts are summed, variance scales are multiplied.
    """
    composed = copy.deepcopy(baseline)
    composed.metadata.is_baseline = False
    composed.metadata.regime_name = "composed"

    # Compose series profile deltas
    for ap in anomaly_profiles:
        for skey, asp in ap.series_profiles.items():
            if asp.existence == "disappeared":
                continue
            if asp.existence == "emergent":
                # Add emergent series from this anomaly
                composed.series_profiles[skey] = copy.deepcopy(asp)
                continue

            if skey in composed.series_profiles:
                csp = composed.series_profiles[skey]
                # Additive mean shift
                if asp.delta_mean is not None:
                    csp.stats.mean += asp.delta_mean
                # Multiplicative variance scaling (delta_std is additive to std)
                if asp.delta_std is not None:
                    csp.stats.std = max(csp.stats.std + asp.delta_std, 0.01)
                # Counter rate deltas
                if asp.delta_rate_mean is not None and csp.rate_stats:
                    csp.rate_stats.mean += asp.delta_rate_mean
                if asp.delta_rate_std is not None and csp.rate_stats:
                    csp.rate_stats.std = max(csp.rate_stats.std + asp.delta_rate_std, 0.01)

    # Handle disappeared: remove only if ALL anomaly profiles mark as disappeared
    for skey in list(composed.series_profiles.keys()):
        if all(
            skey in ap.series_profiles and ap.series_profiles[skey].existence == "disappeared"
            for ap in anomaly_profiles
        ):
            composed.series_profiles[skey].existence = "disappeared"

    # Compose histogram profile deltas
    for ap in anomaly_profiles:
        for fkey, ahp in ap.histogram_profiles.items():
            if ahp.existence == "disappeared":
                continue
            if ahp.existence == "emergent":
                composed.histogram_profiles[fkey] = copy.deepcopy(ahp)
                continue

            if fkey in composed.histogram_profiles:
                chp = composed.histogram_profiles[fkey]
                if ahp.delta_dist_params:
                    for param_key, delta_val in ahp.delta_dist_params.items():
                        if param_key in chp.dist_params:
                            chp.dist_params[param_key] += delta_val
                if ahp.delta_observations_mean is not None:
                    chp.observations_per_step.mean += ahp.delta_observations_mean

    return composed


def analyze_scenario(scenario_path: str | Path) -> None:
    """Analyze a scenario YAML and print a summary."""
    scenario_path = Path(scenario_path)

    with open(scenario_path) as f:
        config = yaml.safe_load(f)

    profiles_dir = Path(config.get("profiles_dir", "./profiles/"))
    if not profiles_dir.is_absolute():
        profiles_dir = scenario_path.parent / profiles_dir

    scenario = config["scenario"]
    step_seconds = scenario.get("step_seconds", 60)

    # Parse segments
    regime_minutes: dict[str, float] = {}
    total_minutes = 0.0
    n_segments = 0
    n_anomaly_segments = 0
    multi_regime_segments: list[tuple[list[str], float]] = []
    all_regime_names: set[str] = set()

    for segment in scenario["segments"]:
        duration = segment["duration_minutes"]
        n_segments += 1
        total_minutes += duration

        if "regime" in segment:
            regime = segment["regime"]
            regime_names = regime if isinstance(regime, list) else [regime]
        elif "regimes" in segment:
            regime_names = segment["regimes"]
        else:
            continue

        is_anomaly = regime_names != ["baseline"]
        if is_anomaly:
            n_anomaly_segments += 1

        if len(regime_names) > 1:
            multi_regime_segments.append((regime_names, duration))

        for name in regime_names:
            all_regime_names.add(name)
            regime_minutes[name] = regime_minutes.get(name, 0.0) + duration

    # Format duration
    def fmt_duration(minutes: float) -> str:
        total_secs = int(minutes * 60)
        days, rem = divmod(total_secs, 86400)
        hours, rem = divmod(rem, 3600)
        mins, _ = divmod(rem, 60)
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours:
            parts.append(f"{hours}h")
        if mins or not parts:
            parts.append(f"{mins}m")
        return " ".join(parts)

    # Print summary
    print(f"Scenario: {scenario_path.name}")
    print(f"Total duration: {fmt_duration(total_minutes)}")
    print(f"Step: {step_seconds}s")
    print(f"Segments: {n_segments} total, {n_segments - n_anomaly_segments} baseline, {n_anomaly_segments} anomaly")
    print()

    # Per-regime breakdown
    print("Regime breakdown:")
    sorted_regimes = sorted(
        regime_minutes.items(),
        key=lambda x: (x[0] != "baseline", -x[1]),
    )
    for name, mins in sorted_regimes:
        pct = mins / total_minutes * 100
        label = "baseline" if name == "baseline" else "anomaly"
        print(f"  {name:40s} {fmt_duration(mins):>10s}  {pct:5.1f}%  ({label})")
    print()

    # Multi-regime segments
    if multi_regime_segments:
        print("Multi-regime segments:")
        for names, dur in multi_regime_segments:
            print(f"  [{', '.join(names)}] — {fmt_duration(dur)}")
        print()

    # Check profiles exist (metric + trace + log)
    missing = []
    found_profiles: dict[str, Path] = {}
    found_trace_profiles: dict[str, Path] = {}
    found_log_profiles: dict[str, Path] = {}
    for name in sorted(all_regime_names):
        path = profiles_dir / f"{name}.profile.json"
        if path.exists():
            found_profiles[name] = path
        else:
            missing.append(name)
        trace_path = profiles_dir / f"{name}.trace.profile.json"
        if trace_path.exists():
            found_trace_profiles[name] = trace_path
        log_path = profiles_dir / f"{name}.log.profile.json"
        if log_path.exists():
            found_log_profiles[name] = log_path

    if missing:
        print(f"MISSING metric profiles ({len(missing)}):")
        for name in missing:
            print(f"  {profiles_dir / name}.profile.json")
        print()

    # Estimated output size from baseline profile
    if "baseline" in found_profiles:
        bp = RegimeProfile.load(found_profiles["baseline"])
        n_series = bp.metadata.n_series
        n_hist = bp.metadata.n_histogram_families
        total_points = int(total_minutes * 60 / step_seconds)
        n_bucket_rows = 0
        for hp in bp.histogram_profiles.values():
            n_bucket_rows += len(hp.le_boundaries) + 1 + 2
        est_rows = (n_series * total_points) + (n_bucket_rows * total_points)
        print(f"Estimated metrics: ~{est_rows:,} rows ({n_series} series + {n_hist} histogram families, {total_points} points)")
    elif not missing:
        print("(Could not estimate output size — no baseline profile found)")

    # Log profile summary
    if found_log_profiles:
        print()
        print(f"Log profiles ({len(found_log_profiles)}):")
        for name, path in sorted(found_log_profiles.items()):
            from otel_synth.trace_config import LogRegimeProfile
            try:
                lp = LogRegimeProfile.load(path)
                print(f"  {name:40s} {len(lp.log_templates)} log templates")
            except Exception as e:
                print(f"  {name:40s} ERROR: {e}")

    # Trace profile summary
    if found_trace_profiles:
        print()
        print(f"Trace profiles ({len(found_trace_profiles)}):")
        for name, path in sorted(found_trace_profiles.items()):
            try:
                tp = TraceRegimeProfile.load(path)
                n_templates = len(tp.trace_templates)
                rate = tp.total_request_rate
                print(
                    f"  {name:40s} {n_templates} trace templates, "
                    f"{rate:.2f} req/s"
                )
                # Show top templates
                sorted_templates = sorted(
                    tp.trace_templates.items(),
                    key=lambda x: x[1].weight,
                    reverse=True,
                )
                for tname, tmpl in sorted_templates[:5]:
                    error_tag = " [ERROR]" if tmpl.is_error_variant else ""
                    print(
                        f"    {tname:36s} weight={tmpl.weight:.3f} "
                        f"n={tmpl.n_instances}{error_tag}"
                    )
            except Exception as e:
                print(f"  {name:40s} (error loading: {e})")
    else:
        print()
        print("No trace profiles found (metrics-only mode)")

    print()
    print(f"Profiles dir: {profiles_dir}")
    if not missing:
        print("All metric profiles found.")
    if found_trace_profiles:
        missing_trace = [n for n in all_regime_names if n not in found_trace_profiles]
        if missing_trace:
            print(f"Missing trace profiles: {', '.join(sorted(missing_trace))}")
        else:
            print("All trace profiles found.")
