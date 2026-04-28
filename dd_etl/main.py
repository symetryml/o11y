"""Orchestration entry point for the Datadog ETL pipeline.

Provides the same public API surface as otel_etl:
    - run_profiler()          → generates schema YAML from Datadog API
    - denormalize_metrics()   → transforms raw metrics → ML-ready wide DataFrame
    - start_receiver()        → launches the FastAPI intake server
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from otel_etl.main import denormalize_metrics as _otel_denormalize
from otel_etl.main import run_profiler_from_dataframe
from otel_etl.profiler.schema_generator import (
    generate_schema,
    save_schema,
    load_schema,
    SchemaConfig,
)
from otel_etl.config.defaults import (
    DEFAULT_CARDINALITY_THRESHOLDS,
    DEFAULT_PROFILING_WINDOW_HOURS,
    DEFAULT_TOP_N,
    CardinalityThresholds,
)

from dd_etl.config.defaults import DEFAULT_RECEIVER_HOST, DEFAULT_RECEIVER_PORT

logger = logging.getLogger(__name__)


def run_profiler(
    dd_api_key: str | None = None,
    dd_app_key: str | None = None,
    dd_site: str = "datadoghq.com",
    output_path: str = "dd_schema_config.yaml",
    profiling_window_hours: float = DEFAULT_PROFILING_WINDOW_HOURS,
    cardinality_thresholds: dict[str, int] | None = None,
    top_n: int = DEFAULT_TOP_N,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
) -> SchemaConfig:
    """Run the profiler against Datadog API to generate a schema config.

    Queries Datadog to discover metrics, tags, and cardinality,
    then generates a YAML schema compatible with otel_etl.

    Args:
        dd_api_key: Datadog API key (or set DD_API_KEY env var).
        dd_app_key: Datadog application key (or set DD_APP_KEY env var).
        dd_site: Datadog site (e.g. datadoghq.com, datadoghq.eu).
        output_path: Path to write schema YAML.
        profiling_window_hours: Lookback window for profiling.
        cardinality_thresholds: Custom tier thresholds.
        top_n: Number of top values to capture per high-cardinality tag.
        include_patterns: Regex patterns to include metric names.
        exclude_patterns: Regex patterns to exclude metric names.

    Returns:
        Generated SchemaConfig dict.
    """
    from dd_etl.utils.datadog_api_client import DatadogClient
    from dd_etl.profiler.metric_discovery import discover_metrics
    from dd_etl.profiler.label_discovery import discover_labels
    from dd_etl.profiler.cardinality_analyzer import analyze_cardinality

    logger.info("Starting DD profiler")

    thresholds: CardinalityThresholds = DEFAULT_CARDINALITY_THRESHOLDS.copy()
    if cardinality_thresholds:
        thresholds.update(cardinality_thresholds)

    client = DatadogClient(
        api_key=dd_api_key,
        app_key=dd_app_key,
        site=dd_site,
    )

    logger.info("Discovering metrics from Datadog API...")
    families = discover_metrics(client, include_patterns, exclude_patterns)
    logger.info(f"Found {len(families)} metric families")

    logger.info("Discovering tags...")
    labels_by_family = discover_labels(client, families)

    logger.info("Analyzing cardinality...")
    cardinality_results = analyze_cardinality(
        client, labels_by_family, thresholds, top_n
    )

    logger.info("Generating schema...")
    schema = generate_schema(
        families, cardinality_results, thresholds, profiling_window_hours
    )

    save_schema(schema, output_path)
    logger.info(f"Schema saved to {output_path}")

    return schema


def run_profiler_from_receiver(
    receiver_url: str = "http://localhost:8126",
    output_path: str = "dd_schema_config.yaml",
    top_n: int = DEFAULT_TOP_N,
    profiling_window_hours: float = DEFAULT_PROFILING_WINDOW_HOURS,
) -> SchemaConfig:
    """Run the profiler against a live dd-etl receiver's in-memory buffer.

    No DD API keys needed — analyzes whatever data the receiver has collected.

    Args:
        receiver_url: URL of the dd-etl receiver.
        output_path: Path to write schema YAML.
        top_n: Number of top values to capture per high-cardinality tag.
        profiling_window_hours: Recorded in the schema metadata.

    Returns:
        Generated SchemaConfig dict.
    """
    import requests

    logger.info("Profiling from receiver at %s", receiver_url)

    resp = requests.get(f"{receiver_url}/profile", params={"top_n": top_n})
    resp.raise_for_status()
    profile_data = resp.json()

    families = profile_data["families"]
    cardinality_results = profile_data["cardinality_results"]
    thresholds = profile_data.get("thresholds", DEFAULT_CARDINALITY_THRESHOLDS.copy())

    logger.info(
        "Receiver profile: %d families, %d rows analyzed",
        len(families),
        profile_data.get("total_rows_analyzed", 0),
    )

    schema = generate_schema(
        families, cardinality_results, thresholds, profiling_window_hours
    )

    save_schema(schema, output_path)
    logger.info("Schema saved to %s", output_path)

    return schema


def denormalize_metrics(
    raw_df: pd.DataFrame,
    schema_config: str | SchemaConfig | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Transform raw metrics into ML-ready wide-format DataFrame.

    Delegates to otel_etl.denormalize_metrics() — the pipeline is
    data-source-agnostic once data is in the standard format.

    Args:
        raw_df: DataFrame with columns: timestamp, metric, labels (dict), value.
        schema_config: Schema config path or object.
        **kwargs: Passed through to otel_etl.denormalize_metrics().

    Returns:
        Wide-format DataFrame with features as columns.
    """
    df = raw_df.copy()
    # When loaded from CSV, labels and timestamps may be strings — normalize
    if not df.empty:
        if isinstance(df["labels"].iloc[0], str):
            import ast
            df["labels"] = df["labels"].apply(ast.literal_eval)
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"])
    return _otel_denormalize(df, schema_config=schema_config, **kwargs)


def start_receiver(
    host: str = DEFAULT_RECEIVER_HOST,
    port: int = DEFAULT_RECEIVER_PORT,
    checkpoint_path: str = ".dd_etl_checkpoint.json",
    retention_hours: int = 24,
) -> None:
    """Start the FastAPI receiver as a blocking server.

    Args:
        host: Bind address.
        port: Bind port.
        checkpoint_path: Where to store the checkpoint.
        retention_hours: How long to keep data in memory.
    """
    import uvicorn
    from dd_etl.receiver.app import create_app

    app = create_app(
        checkpoint_path=checkpoint_path,
        retention_hours=retention_hours,
    )
    uvicorn.run(app, host=host, port=port)
