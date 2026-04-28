"""FastAPI application that mimics the Datadog intake API.

The DD agent sends metric payloads here via ``additional_endpoints``.
Payloads are parsed and stored in the MetricStore for later querying.

Usage::

    from dd_etl.receiver.app import create_app
    import uvicorn

    app = create_app(storage_dir="./dd_metrics_store")
    uvicorn.run(app, host="0.0.0.0", port=8126)
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import zlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Query, Request, Response

from dd_etl.receiver.payload_parser import parse_v1_series, parse_v2_series, parse_v2_protobuf, parse_intake
from dd_etl.receiver.metric_store import MetricStore
from dd_etl.utils.checkpoint import Checkpoint

logger = logging.getLogger(__name__)


async def _read_body(request: Request) -> dict:
    """Read request body, decompress if needed, parse JSON.

    The OTel Collector Datadog exporter sends gzip-compressed payloads.
    """
    raw = await request.body()
    encoding = request.headers.get("content-encoding", "").lower()

    if encoding == "gzip":
        raw = gzip.decompress(raw)
    elif encoding == "deflate":
        raw = zlib.decompress(raw)
    elif encoding == "zstd":
        try:
            import zstandard as zstd
            raw = zstd.ZstdDecompressor().decompress(raw)
        except ImportError:
            logger.warning("zstd payload received but zstandard not installed")
            return {}

    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error("Failed to parse JSON (%s), body[:200]: %r", e, raw[:200])
        return {}


# Module-level store reference so endpoints can access it
_store: MetricStore | None = None
_checkpoint: Checkpoint | None = None
_flush_task: asyncio.Task | None = None


def create_app(
    checkpoint_path: str = ".dd_etl_checkpoint.json",
    checkpoint_interval_seconds: int = 60,
    retention_hours: int = 24,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        checkpoint_path: Path for the high-water-mark checkpoint file.
        checkpoint_interval_seconds: How often to update the checkpoint.
        retention_hours: How long to keep data in memory.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global _store, _checkpoint, _flush_task
        _store = MetricStore(retention_hours=retention_hours)
        _checkpoint = Checkpoint(checkpoint_path)
        _flush_task = asyncio.create_task(
            _periodic_checkpoint(_checkpoint, checkpoint_interval_seconds)
        )
        logger.info("DD receiver started (in-memory buffer)")
        yield
        _flush_task.cancel()
        if _checkpoint:
            _checkpoint.update(datetime.now(timezone.utc))
        logger.info("DD receiver shut down")

    app = FastAPI(title="DD Metrics Receiver", lifespan=lifespan)

    # ------------------------------------------------------------------
    # Intake endpoints
    # ------------------------------------------------------------------

    @app.post("/api/v1/series")
    async def intake_v1_series(request: Request) -> dict:
        payload = await _read_body(request)
        if not payload:
            return {"status": "ok", "accepted": 0}
        rows = parse_v1_series(payload)
        _store.append(rows)
        return {"status": "ok", "accepted": len(rows)}

    @app.post("/api/v2/series")
    async def intake_v2_series(request: Request) -> dict:
        content_type = request.headers.get("content-type", "")
        if "protobuf" in content_type:
            raw = await request.body()
            encoding = request.headers.get("content-encoding", "").lower()
            if encoding == "gzip":
                raw = gzip.decompress(raw)
            elif encoding == "deflate":
                raw = zlib.decompress(raw)
            rows = parse_v2_protobuf(raw)
        else:
            payload = await _read_body(request)
            if not payload:
                return {"status": "ok", "accepted": 0}
            rows = parse_v2_series(payload)
        _store.append(rows)
        return {"status": "ok", "accepted": len(rows)}

    @app.post("/intake/")
    async def general_intake(request: Request) -> dict:
        payload = await _read_body(request)
        if not payload:
            return {"status": "ok", "accepted": 0}
        rows = parse_intake(payload)
        _store.append(rows)
        return {"status": "ok", "accepted": len(rows)}

    # The DD exporter also sends sketches (distribution metrics) — accept silently
    @app.post("/api/beta/sketches")
    async def intake_sketches(request: Request) -> dict:
        # Sketches are protobuf-encoded distribution data; log and discard for now
        raw = await request.body()
        logger.debug("Received sketches payload (%d bytes) — discarding", len(raw))
        return {"status": "ok"}

    # The DD agent/exporter sends validate, check_run, metadata — accept GET and POST
    @app.api_route("/api/v1/validate", methods=["GET", "POST"])
    async def validate(request: Request) -> dict:
        return {"valid": True}

    @app.api_route("/api/v1/check_run", methods=["GET", "POST"])
    async def check_run(request: Request) -> dict:
        return {"status": "ok"}

    @app.api_route("/api/v1/metadata", methods=["GET", "POST"])
    async def metadata(request: Request) -> dict:
        return {"status": "ok"}

    # ------------------------------------------------------------------
    # Utility endpoints
    # ------------------------------------------------------------------

    @app.get("/health")
    async def health() -> dict:
        return {
            "status": "ok",
            "metrics_buffered": _store.buffered_count() if _store else 0,
            "metrics_known": len(_store.get_metric_names()) if _store else 0,
            "last_checkpoint": (
                _checkpoint.get_last_seen().isoformat()
                if _checkpoint and _checkpoint.get_last_seen()
                else None
            ),
        }

    @app.get("/metrics")
    async def list_metrics() -> dict:
        return {
            "metrics": _store.get_metric_names() if _store else [],
            "type_registry": _store.get_type_registry() if _store else {},
        }

    @app.get("/query")
    async def query_metrics(
        start: str | None = Query(None, description="ISO8601 start time"),
        end: str | None = Query(None, description="ISO8601 end time"),
        step: str = Query("60s", description="Re-aggregation step, e.g. 60s, 5m"),
        metric: list[str] | None = Query(None, description="Metric names to include"),
    ) -> dict:
        """Query the metric store and return re-aggregated data as JSON."""
        if not _store:
            return {"rows": 0, "data": []}

        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        df = _store.fetch_metrics_range(
            metric_names=metric or None,
            start=start_dt,
            end=end_dt,
            step=step,
        )

        if df.empty:
            return {"rows": 0, "data": []}

        # Convert to JSON-serializable format
        records = []
        for _, row in df.iterrows():
            records.append({
                "timestamp": row["timestamp"].isoformat(),
                "metric": row["metric"],
                "labels": row["labels"],
                "value": row["value"],
            })

        return {"rows": len(records), "data": records}

    @app.get("/profile")
    async def profile_metrics(
        top_n: int = Query(20, description="Number of top values to capture per label"),
    ) -> dict:
        """Analyze the buffer to produce profiling data for schema generation.

        Returns metric families, label cardinality, tier assignments, and
        top values — everything needed by generate_schema().
        """
        if not _store:
            return {"families": {}, "cardinality_results": {}}

        with _store._lock:
            rows = list(_store._buffer)

        if not rows:
            return {"families": {}, "cardinality_results": {}}

        type_registry = _store.get_type_registry()

        # Build per-metric label analysis
        from collections import Counter, defaultdict
        from otel_etl.config.defaults import DEFAULT_CARDINALITY_THRESHOLDS, get_tier, get_action
        from otel_etl.utils.name_sanitizer import extract_metric_family, classify_metric_type

        # Collect all label values per (metric, label_key)
        metric_labels: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))

        for row in rows:
            metric = row["metric"]
            labels = row.get("labels", {})
            if isinstance(labels, str):
                labels = json.loads(labels)
            for k, v in labels.items():
                metric_labels[metric][k][v] += 1

        # Build families
        families = {}
        for metric_name in sorted(type_registry.keys()):
            family_name = extract_metric_family(metric_name)
            otel_type = classify_metric_type(metric_name)
            # Map dd_type to otel type if classify_metric_type returns gauge
            dd_type = type_registry.get(metric_name, "gauge")
            if dd_type == "count" and otel_type == "gauge":
                otel_type = "counter"

            if family_name not in families:
                families[family_name] = {
                    "name": family_name,
                    "type": otel_type,
                    "metrics": [],
                }
            families[family_name]["metrics"].append(metric_name)

        # Build cardinality results
        thresholds = DEFAULT_CARDINALITY_THRESHOLDS
        cardinality_results = {}

        for family_name, family in families.items():
            family_cardinality = {}
            # Merge labels across all metrics in the family
            merged_labels: dict[str, Counter] = defaultdict(Counter)
            for metric_name in family["metrics"]:
                for label_key, value_counts in metric_labels.get(metric_name, {}).items():
                    merged_labels[label_key].update(value_counts)

            for label_key, value_counts in merged_labels.items():
                cardinality = len(value_counts)
                tier = get_tier(cardinality, thresholds)
                action = get_action(tier)
                top_values = [v for v, _ in value_counts.most_common(top_n)]

                family_cardinality[label_key] = {
                    "label": label_key,
                    "cardinality": cardinality,
                    "tier": tier,
                    "action": action,
                    "top_values": top_values if action == "top_n" else None,
                }

            cardinality_results[family_name] = family_cardinality

        return {
            "families": families,
            "cardinality_results": cardinality_results,
            "thresholds": thresholds,
            "total_metrics": len(type_registry),
            "total_rows_analyzed": len(rows),
        }

    return app


async def _periodic_checkpoint(
    checkpoint: Checkpoint,
    interval: int,
) -> None:
    """Background task that updates the checkpoint periodically."""
    while True:
        await asyncio.sleep(interval)
        try:
            checkpoint.update(datetime.now(timezone.utc))
        except Exception as e:
            logger.error(f"Checkpoint update failed: {e}")
