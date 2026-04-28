#!/usr/bin/env python3
"""Generate expected test results from Python denormalize_metrics for Go parity testing."""

import json
import math
import sys
import os

# Add the o11y root to path so we can import otel_etl
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import numpy as np
import pandas as pd
from otel_etl.main import denormalize_metrics
from otel_etl.transformer.status_bucketer import bucket_status_code
from otel_etl.transformer.method_bucketer import bucket_http_method
from otel_etl.transformer.operation_bucketer import bucket_operation
from otel_etl.transformer.route_parameterizer import parameterize_route
from otel_etl.profiler.semantic_classifier import classify_label, is_entity_label
from otel_etl.utils.name_sanitizer import extract_metric_family, classify_metric_type
from otel_etl.aggregator.histogram_agg import estimate_percentile_from_buckets


def nan_safe(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (np.floating,)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    if isinstance(v, (np.int64, np.int32)):
        return int(v)
    return v


def series_to_list(s):
    return [nan_safe(v) for v in s.tolist()]


results = {}

# --- Transformer tests ---

# Status bucketing
results["status_bucket"] = {
    "http_200": bucket_status_code("200", "http_status_code"),
    "http_404": bucket_status_code("404", "http_status_code"),
    "http_500": bucket_status_code("500", "http_status_code"),
    "grpc_0": bucket_status_code("0", "grpc_status"),
    "grpc_13": bucket_status_code("13", "grpc_status"),
    "auto_200": bucket_status_code("200", ""),
    "auto_0": bucket_status_code("0", ""),
}

# Method bucketing
results["method_bucket"] = {
    "GET": bucket_http_method("GET"),
    "POST": bucket_http_method("POST"),
    "DELETE": bucket_http_method("DELETE"),
    "HEAD": bucket_http_method("HEAD"),
    "CONNECT": bucket_http_method("CONNECT"),
    "WEIRD": bucket_http_method("WEIRD"),
}

# Operation bucketing
results["operation_bucket"] = {
    "SELECT": bucket_operation("SELECT * FROM users"),
    "INSERT": bucket_operation("INSERT INTO orders"),
    "GetUser": bucket_operation("GetUser"),
    "CreateOrder": bucket_operation("CreateOrder"),
    "StreamEvents": bucket_operation("StreamEvents"),
}

# Route parameterization
results["route_param"] = {
    "uuid": parameterize_route("/api/users/550e8400-e29b-41d4-a716-446655440000/profile"),
    "numeric_id": parameterize_route("/api/orders/12345/items"),
    "date": parameterize_route("/api/reports/2024-01-15"),
    "plain": parameterize_route("/api/health"),
}

# --- Classifier tests ---

results["classify_label"] = {
    "service_name": {"category": classify_label("service_name")["category"].value,
                     "bucket_type": classify_label("service_name")["bucket_type"] or ""},
    "status_code": {"category": classify_label("status_code")["category"].value,
                    "bucket_type": classify_label("status_code")["bucket_type"] or ""},
    "http_method": {"category": classify_label("http_method")["category"].value,
                    "bucket_type": classify_label("http_method")["bucket_type"] or ""},
    "trace_id": {"category": classify_label("trace_id")["category"].value,
                 "bucket_type": classify_label("trace_id")["bucket_type"] or ""},
    "le": {"category": classify_label("le")["category"].value,
           "bucket_type": classify_label("le")["bucket_type"] or ""},
}

results["is_entity_label"] = {
    "service_name": is_entity_label("service_name"),
    "instance": is_entity_label("instance"),
    "trace_id": is_entity_label("trace_id"),
    "http_method": is_entity_label("http_method"),
}

results["metric_family"] = {
    "http_requests_total": extract_metric_family("http_requests_total"),
    "http_request_duration_bucket": extract_metric_family("http_request_duration_bucket"),
    "http_request_duration_sum": extract_metric_family("http_request_duration_sum"),
    "cpu_usage": extract_metric_family("cpu_usage"),
}

results["metric_type"] = {
    "http_requests_total": classify_metric_type("http_requests_total"),
    "http_request_duration_bucket": classify_metric_type("http_request_duration_bucket"),
    "http_request_duration_sum": classify_metric_type("http_request_duration_sum"),
    "cpu_usage": classify_metric_type("cpu_usage"),
}

# --- Histogram percentile estimation ---
boundaries = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float("inf")]
counts = [0, 0, 0, 2, 5, 15, 45, 80, 95, 99, 100, 100]
results["histogram_percentiles"] = {
    "p50": estimate_percentile_from_buckets(boundaries, counts, 0.50),
    "p90": estimate_percentile_from_buckets(boundaries, counts, 0.90),
    "p99": estimate_percentile_from_buckets(boundaries, counts, 0.99),
}

# --- Full denormalize_metrics pipeline test ---

# Build a small but realistic test dataset
raw_data = []

# Gauge metrics
for i, ts in enumerate(["2024-01-01T00:00:00", "2024-01-01T00:01:00"]):
    raw_data.append({"timestamp": ts, "metric": "cpu_usage", "labels": {"service_name": "web", "instance": "i-1"}, "value": 45.0 + i * 5})
    raw_data.append({"timestamp": ts, "metric": "memory_usage", "labels": {"service_name": "web", "instance": "i-1"}, "value": 70.0 + i * 2})

# Counter metrics
for i, ts in enumerate(["2024-01-01T00:00:00", "2024-01-01T00:01:00"]):
    raw_data.append({"timestamp": ts, "metric": "http_requests_total", "labels": {"service_name": "web", "status_code": "200"}, "value": 100.0 + i * 50})
    raw_data.append({"timestamp": ts, "metric": "http_requests_total", "labels": {"service_name": "web", "status_code": "500"}, "value": 5.0 + i * 2})

# Histogram metrics (single timestamp)
ts = "2024-01-01T00:00:00"
for le, count in [("0.01", 5), ("0.05", 20), ("0.1", 50), ("0.5", 85), ("1.0", 95), ("+Inf", 100)]:
    raw_data.append({"timestamp": ts, "metric": "http_request_duration_bucket", "labels": {"service_name": "web", "le": le}, "value": float(count)})
raw_data.append({"timestamp": ts, "metric": "http_request_duration_sum", "labels": {"service_name": "web"}, "value": 45.5})
raw_data.append({"timestamp": ts, "metric": "http_request_duration_count", "labels": {"service_name": "web"}, "value": 100.0})

raw_df = pd.DataFrame(raw_data)
raw_df["timestamp"] = pd.to_datetime(raw_df["timestamp"])

# Run denormalize_metrics with minimal config (no schema, no deltas for simplicity)
wide_df = denormalize_metrics(raw_df, include_deltas=False)

results["denormalize_full"] = {
    "nrows": len(wide_df),
    "ncols": len(wide_df.columns),
    "columns": sorted([c for c in wide_df.columns if c not in ["timestamp", "entity_key"]]),
    "has_timestamp": "timestamp" in wide_df.columns,
    "has_entity_key": "entity_key" in wide_df.columns,
}

# Check specific values if present
for col in wide_df.columns:
    if col in ("timestamp", "entity_key"):
        continue
    vals = series_to_list(wide_df[col])
    results["denormalize_full"][f"col_{col}"] = vals

# --- Smaller focused pipeline test (gauges only) ---
gauge_data = []
for ts in ["2024-01-01T00:00:00", "2024-01-01T00:01:00"]:
    gauge_data.append({"timestamp": ts, "metric": "cpu_usage", "labels": {"service_name": "api"}, "value": 50.0 if ts.endswith("00:00") else 60.0})
    gauge_data.append({"timestamp": ts, "metric": "memory_usage", "labels": {"service_name": "api"}, "value": 70.0 if ts.endswith("00:00") else 75.0})

gauge_df = pd.DataFrame(gauge_data)
gauge_df["timestamp"] = pd.to_datetime(gauge_df["timestamp"])

gauge_wide = denormalize_metrics(gauge_df, include_deltas=False)

results["denormalize_gauges"] = {
    "nrows": len(gauge_wide),
    "ncols": len(gauge_wide.columns),
    "columns": sorted([c for c in gauge_wide.columns if c not in ["timestamp", "entity_key"]]),
}

for col in gauge_wide.columns:
    if col in ("timestamp", "entity_key"):
        continue
    results["denormalize_gauges"][f"col_{col}"] = series_to_list(gauge_wide[col])

with open(os.path.join(os.path.dirname(__file__), "expected.json"), "w") as f:
    json.dump(results, f, indent=2)

print(f"Generated {len(results)} test cases → testdata/expected.json")
