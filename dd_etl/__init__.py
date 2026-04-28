"""
DD Metrics ETL — Datadog Agent metrics → ML-ready wide-format DataFrames.

Intercepts Datadog Agent payloads via ``additional_endpoints``, buffers
them locally, and transforms through the otel_etl pipeline.

Public API:
    - run_profiler():       Generate schema config from Datadog API
    - denormalize_metrics(): Transform raw metrics into wide-format DataFrame
    - start_receiver():     Launch the FastAPI intake server
    - MetricStore:          Direct access to the buffered metric store
    - filter_by_service():  Re-exported from otel_etl
"""

from dd_etl.main import (
    run_profiler,
    run_profiler_from_receiver,
    run_profiler_from_dataframe,
    denormalize_metrics,
    start_receiver,
)
from dd_etl.receiver.metric_store import MetricStore
from otel_etl.utils.filters import filter_by_service

__all__ = [
    "run_profiler",
    "run_profiler_from_receiver",
    "run_profiler_from_dataframe",
    "denormalize_metrics",
    "start_receiver",
    "MetricStore",
    "filter_by_service",
]
__version__ = "0.1.0"
