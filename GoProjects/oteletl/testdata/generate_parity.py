#!/usr/bin/env python3
"""Generate parity test data from real Prometheus export.

Loads baseline10m.csv, runs denormalize_metrics, and saves both the raw input
(sampled to keep test size manageable) and the expected output as JSON for
Go parity testing.
"""

import ast
import json
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import numpy as np
import pandas as pd
from otel_etl.main import denormalize_metrics
from otel_etl.utils.filters import filter_by_service, filter_salient_metrics

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "otel_flagd", "fetch_results", "baseline10m.csv"
)
OUT_DIR = os.path.dirname(__file__)


def nan_safe(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (np.floating,)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    if isinstance(v, (np.int64, np.int32)):
        return int(v)
    if isinstance(v, np.bool_):
        return bool(v)
    return v


def df_to_json_rows(df):
    """Convert a raw metrics DataFrame to JSON-serialisable rows."""
    rows = []
    for _, row in df.iterrows():
        labels = row["labels"]
        if isinstance(labels, str):
            labels = ast.literal_eval(labels)
        rows.append({
            "timestamp": str(row["timestamp"]),
            "metric": row["metric"],
            "labels": labels,
            "value": nan_safe(row["value"]),
        })
    return rows


def wide_to_json(df):
    """Convert wide-format output to JSON-serialisable dict of columns."""
    result = {
        "nrows": len(df),
        "ncols": len(df.columns),
        "columns": sorted([c for c in df.columns if c not in ("timestamp", "entity_key")]),
    }
    for col in df.columns:
        if col in ("timestamp", "entity_key"):
            result[f"col_{col}"] = [str(v) for v in df[col].tolist()]
        else:
            result[f"col_{col}"] = [nan_safe(v) for v in df[col].tolist()]
    return result


def main():
    print(f"Loading {CSV_PATH}...")
    raw_df = pd.read_csv(CSV_PATH)
    raw_df["timestamp"] = pd.to_datetime(raw_df["timestamp"])
    # Parse string labels to dicts
    raw_df["labels"] = raw_df["labels"].apply(ast.literal_eval)
    print(f"  Loaded {len(raw_df)} rows, {raw_df['metric'].nunique()} unique metrics")

    # --- Test 1: Single service, small slice ---
    # Pick "accounting" service, first 2 minutes of data
    svc_df = filter_by_service(raw_df, "accounting")
    ts_min = svc_df["timestamp"].min()
    ts_max = ts_min + pd.Timedelta(minutes=2)
    slice_df = svc_df[(svc_df["timestamp"] >= ts_min) & (svc_df["timestamp"] <= ts_max)]
    print(f"  Test 1 (accounting, 2min): {len(slice_df)} rows")

    wide1 = denormalize_metrics(slice_df, include_deltas=False)
    print(f"  Output: {len(wide1)} rows, {len(wide1.columns)} columns")

    test1 = {
        "input": df_to_json_rows(slice_df),
        "expected": wide_to_json(wide1),
    }

    # --- Test 2: Multi-service, salient metrics ---
    # Pick 2 services, filter to salient metrics, 2 minutes
    multi_df = filter_by_service(raw_df, ["accounting", "frontend-proxy"])
    multi_df = multi_df[
        (multi_df["timestamp"] >= ts_min) & (multi_df["timestamp"] <= ts_max)
    ]
    metric_names = multi_df["metric"].unique().tolist()
    salient = filter_salient_metrics(metric_names)
    multi_df = multi_df[multi_df["metric"].isin(salient)]
    print(f"  Test 2 (multi-svc, salient, 2min): {len(multi_df)} rows, {len(salient)} salient metrics")

    wide2 = denormalize_metrics(multi_df, include_deltas=False)
    print(f"  Output: {len(wide2)} rows, {len(wide2.columns)} columns")

    test2 = {
        "input": df_to_json_rows(multi_df),
        "expected": wide_to_json(wide2),
    }

    # --- Test 3: With deltas ---
    # Use the accounting slice but include deltas
    wide3 = denormalize_metrics(slice_df, include_deltas=True, layers=[1])
    print(f"  Test 3 (with deltas): {len(wide3)} rows, {len(wide3.columns)} columns")

    test3 = {
        "input": df_to_json_rows(slice_df),
        "expected": wide_to_json(wide3),
    }

    # Save
    output = {
        "single_service": test1,
        "multi_service_salient": test2,
        "with_deltas": test3,
    }

    out_path = os.path.join(OUT_DIR, "parity.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    size_mb = os.path.getsize(out_path) / 1024 / 1024
    print(f"\nSaved {len(output)} parity tests → {out_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
