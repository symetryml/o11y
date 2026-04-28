"""
signals — Unified telemetry data access layer for traces, logs, and metrics.

Usage:
    from signals import fetch_traces, fetch_logs, PrometheusClient
    from signals import list_services, list_operations, get_trace_by_id
    from signals import search_logs, get_log_statistics
    from signals import fetch_metrics_range_df, iter_metrics_windows
"""

# Trace API (Jaeger gRPC)
from signals.traces import (
    list_services,
    list_operations,
    fetch_traces,
    get_trace_by_id,
    aggregate_spans_to_traces,
)

# Log API (OpenSearch)
from signals.logs import (
    fetch_logs,
    search_logs,
    get_log_statistics,
)

# Metrics API (Prometheus + Datadog)
from signals.metrics import (
    PrometheusClient,
    DatadogClient,
    fetch_metrics_range_df,
    iter_metrics_windows,
    get_metrics_dataframe2,
    get_prometheus_scrape_interval,
    detect_scrape_interval,
)
