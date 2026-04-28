"""Datadog-specific configuration defaults."""

# ---------------------------------------------------------------------------
# Tag name mapping: Datadog convention -> OTel / otel_etl convention
# Keys not listed here pass through unchanged.
# ---------------------------------------------------------------------------
DD_TAG_MAPPING: dict[str, str] = {
    "service": "service_name",
    "host": "instance",
    "env": "environment",
    "version": "service_version",
    "source": "telemetry_source",
}

# ---------------------------------------------------------------------------
# Metric type mapping
#   dd_type -> (name_suffix_to_append, otel_equivalent_type)
#
# "count" metrics get "_total" appended so otel_etl's classify_metric_type()
# routes them through the counter aggregation path.
# "rate" and "gauge" stay as-is -> routed to gauge aggregation.
# ---------------------------------------------------------------------------
DD_TYPE_MAPPING: dict[str, tuple[str | None, str]] = {
    "gauge": (None, "gauge"),
    "count": ("_total", "counter"),
    "rate": (None, "gauge"),
    "distribution": (None, "gauge"),
}

# ---------------------------------------------------------------------------
# Datadog histogram sub-metric suffixes
#   When the DD agent pre-aggregates histograms, it emits 5 sub-metrics.
#   We map each suffix to the corresponding aggregation name used in feature
#   naming (so they show up correctly in wide-format columns).
# ---------------------------------------------------------------------------
DD_HISTOGRAM_SUFFIXES: dict[str, str] = {
    ".avg": "mean",
    ".count": "count",
    ".median": "p50",
    ".max": "max",
    ".95percentile": "p95",
}

# ---------------------------------------------------------------------------
# Agent flush / receiver defaults
# ---------------------------------------------------------------------------
DEFAULT_FLUSH_INTERVAL_SECONDS: int = 10
DEFAULT_BUFFER_RETENTION_HOURS: int = 24
DEFAULT_PARQUET_PARTITION_MINUTES: int = 60
DEFAULT_RECEIVER_PORT: int = 8126
DEFAULT_RECEIVER_HOST: str = "0.0.0.0"

# ---------------------------------------------------------------------------
# Intake endpoint paths (what the DD agent POSTs to)
# ---------------------------------------------------------------------------
INTAKE_ENDPOINTS: dict[str, str] = {
    "v1_series": "/api/v1/series",
    "v2_series": "/api/v2/series",
    "intake": "/intake/",
    "health": "/health",
    "validate": "/api/v1/validate",
    "check_run": "/api/v1/check_run",
    "metadata": "/api/v1/metadata",
}
