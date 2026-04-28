#!/usr/bin/env python3
"""Generate parity test data using the real checkout service workflow.

Mirrors the exact Python pipeline:
1. filter_by_service(df, ["checkout"])
2. filter_salient_metrics(metrics)
3. iter_metrics_windows(raw, metrics, window_minutes=5, step="60s")
4. denormalize_metrics(window_df, schema_config=schema, entity_labels=["service_name"], ...)
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
from signals.metrics.prometheus import iter_metrics_windows

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "..",
    "otel_flagd", "fetch_results", "baseline10m.csv"
)
SCHEMA_PATH = os.environ.get(
    "SCHEMA_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "schemas", "schema_config-otel001.yaml"),
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
    raw_df["labels"] = raw_df["labels"].apply(ast.literal_eval)
    print(f"  Loaded {len(raw_df)} rows")

    the_service = "checkout"

    # Step 1: Get metrics for checkout service
    all_metrics = raw_df["metric"].unique().tolist()
    checkout_metrics_df = raw_df[
        raw_df["labels"].apply(
            lambda x: x.get("service_name") == the_service or
                       x.get("service") == the_service or
                       x.get("job", "").endswith(the_service)
        )
    ]
    checkout_metric_names = checkout_metrics_df["metric"].unique().tolist()
    print(f"  Checkout metrics: {len(checkout_metric_names)}")

    # Step 2: Filter salient
    salient_metrics = filter_salient_metrics(checkout_metric_names)
    print(f"  Salient metrics: {len(salient_metrics)}")

    # Step 3: Filter by service
    raw_filtered = filter_by_service(raw_df, [the_service])
    print(f"  Rows for checkout: {len(raw_filtered)}")

    # Step 4: Iterate windows and denormalize
    schema = SCHEMA_PATH

    windows_output = []
    itdf = iter_metrics_windows(raw_filtered, metric_names=salient_metrics, window_minutes=5, step="60s")

    for window_start, window_end, window_df in itdf:
        features_df = denormalize_metrics(
            window_df,
            schema_config=schema,
            entity_labels=["service_name"],
            column_registry=None,
            layers=[1, 2, 3],
            window_seconds=60,
            include_deltas=True,
            unique_timestamps=False,
        )

        windows_output.append({
            "window_start": str(window_start),
            "window_end": str(window_end),
            "input_rows": len(window_df),
            "input": df_to_json_rows(window_df),
            "expected": wide_to_json(features_df),
        })

        print(f"  Window {window_start} → {window_end}: "
              f"{len(window_df)} input rows → "
              f"{len(features_df)} output rows, {len(features_df.columns)} cols")

    output = {
        "service": the_service,
        "salient_metrics": sorted(salient_metrics),
        "num_windows": len(windows_output),
        "windows": windows_output,
    }

    out_path = os.path.join(OUT_DIR, "checkout_parity.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    size_mb = os.path.getsize(out_path) / 1024 / 1024
    print(f"\nSaved {len(windows_output)} windows → {out_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
