"""CLI fetch subcommand — fetch metrics, traces, and logs to CSV."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from rich.console import Console

CHUNK_MINUTES = 5


def register(subparsers: argparse._SubParsersAction, parents: list | None = None) -> None:
    fetch_parser = subparsers.add_parser(
        "fetch", help="Fetch metrics/traces/logs to CSV", parents=parents or []
    )
    fetch_parser.add_argument(
        "--url", required=True,
        help="Prometheus base URL (e.g. http://localhost:9090) or dd_etl receiver URL when --use-dd",
    )
    fetch_parser.add_argument(
        "--outfile", required=True, help="Output CSV file path (traces/logs get _traces/_logs suffix)"
    )
    fetch_parser.add_argument(
        "--minutes", type=int, default=60, help="How many minutes of data to fetch (default: 60)"
    )
    fetch_parser.add_argument(
        "--step", default="60s", help="Query resolution step (default: 60s)"
    )
    fetch_parser.add_argument(
        "--retries", type=int, default=3, help="Number of retries on failure (default: 3)"
    )
    fetch_parser.add_argument(
        "--chunk-minutes", type=int, default=CHUNK_MINUTES,
        help=f"Fetch in chunks of N minutes to avoid overloading Prometheus (default: {CHUNK_MINUTES})",
    )
    fetch_parser.add_argument(
        "--use-dd", action="store_true", default=False,
        help="Fetch from a dd_etl receiver instead of Prometheus",
    )
    fetch_parser.add_argument(
        "--jaeger-url", default=None,
        help="Jaeger base URL to fetch traces (e.g. http://localhost:16686). Omit to skip traces.",
    )
    fetch_parser.add_argument(
        "--opensearch-url", default=None,
        help="OpenSearch base URL to fetch logs (e.g. http://localhost:9200). Omit to skip logs.",
    )


def _discover_metrics(get_metrics_dataframe2, prometheus_url: str, max_retries: int):
    """Discover metrics with retries."""
    for attempt in range(1, max_retries + 1):
        try:
            the_metrics_df = get_metrics_dataframe2(prometheus_url)
        except KeyError:
            if attempt < max_retries:
                time.sleep(10 * attempt)
                continue
            raise RuntimeError(
                "Prometheus returned no series data (possibly overloaded or unavailable)"
            )
        if the_metrics_df.empty or "metric" not in the_metrics_df.columns:
            if attempt < max_retries:
                time.sleep(10 * attempt)
                continue
            raise RuntimeError("No metrics found on Prometheus (empty response)")
        return the_metrics_df["metric"].unique()
    raise RuntimeError("Failed to discover metrics")


def _fetch_chunk(client, metric_names, chunk_start, chunk_end, step, max_retries):
    """Fetch a single time chunk with retries."""
    for attempt in range(1, max_retries + 1):
        try:
            return client.fetch_metrics_range(metric_names, chunk_start, chunk_end, step)
        except Exception:
            if attempt < max_retries:
                time.sleep(10 * attempt)
                continue
            raise


def _dd_discover_metrics(receiver_url: str, max_retries: int) -> list[str]:
    """Discover metrics from the dd_etl receiver with retries."""
    import httpx

    for attempt in range(1, max_retries + 1):
        try:
            resp = httpx.get(f"{receiver_url}/metrics", timeout=30)
            resp.raise_for_status()
            data = resp.json()
            metrics = data.get("metrics", [])
            if not metrics:
                if attempt < max_retries:
                    time.sleep(10 * attempt)
                    continue
                raise RuntimeError("No metrics found on dd_etl receiver (empty buffer)")
            return metrics
        except httpx.HTTPError:
            if attempt < max_retries:
                time.sleep(10 * attempt)
                continue
            raise
    raise RuntimeError("Failed to discover metrics from dd_etl receiver")


def _dd_fetch_chunk(receiver_url: str, metric_names, chunk_start, chunk_end, step, max_retries):
    """Fetch a single time chunk from the dd_etl receiver with retries."""
    import httpx

    for attempt in range(1, max_retries + 1):
        try:
            resp = httpx.get(
                f"{receiver_url}/query",
                params={
                    "start": chunk_start.isoformat(),
                    "end": chunk_end.isoformat(),
                    "step": step,
                    "metric": metric_names,
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("data", [])
            if not rows:
                return pd.DataFrame(columns=["timestamp", "metric", "labels", "value"])
            return pd.DataFrame(rows)
        except Exception:
            if attempt < max_retries:
                time.sleep(10 * attempt)
                continue
            raise


def _run_dd(args: argparse.Namespace, console: Console) -> int:
    """Fetch metrics from a dd_etl receiver."""
    output_json = getattr(args, "output_format", "text") == "json"
    receiver_url = args.url.rstrip("/")
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=args.minutes)
    max_retries = getattr(args, "retries", 3)
    chunk_minutes = getattr(args, "chunk_minutes", CHUNK_MINUTES)

    try:
        if not output_json:
            console.print(f"Discovering metrics on dd_etl receiver [cyan]{receiver_url}[/] ...")
        metric_names = _dd_discover_metrics(receiver_url, max_retries)

        if not output_json:
            console.print(f"Found [bold]{len(metric_names)}[/] metrics")

        chunks = []
        chunk_start = start_time
        while chunk_start < end_time:
            chunk_end = min(chunk_start + timedelta(minutes=chunk_minutes), end_time)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end

        all_dfs = []
        for i, (c_start, c_end) in enumerate(chunks, 1):
            chunk_df = _dd_fetch_chunk(
                receiver_url, metric_names, c_start, c_end, args.step, max_retries
            )
            all_dfs.append(chunk_df)
            if not output_json:
                mins = int((c_end - c_start).total_seconds() / 60)
                console.print(
                    f"  Chunk {i}/{len(chunks)}: fetching {mins} min "
                    f"({c_start.strftime('%H:%M')}–{c_end.strftime('%H:%M')}) ..."
                    f"  ({len(chunk_df)} tuples)"
                )

        raw_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
        raw_df.to_csv(args.outfile, index=False)

        if output_json:
            console.print(json.dumps({
                "file": args.outfile,
                "rows": len(raw_df),
                "metrics": len(metric_names),
                "chunks": len(chunks),
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "source": "datadog",
            }))
        else:
            console.print(
                f"[green]Saved [bold]{len(raw_df)}[/] rows to {args.outfile}[/]"
            )
        return 0
    except Exception as e:
        if output_json:
            console.print(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]Error:[/] {e}")
        return 1


def _run_prometheus(args: argparse.Namespace, console: Console) -> int:
    """Fetch metrics from Prometheus via otel_etl."""
    output_json = getattr(args, "output_format", "text") == "json"

    try:
        from signals import PrometheusClient, get_metrics_dataframe2
    except ImportError as e:
        msg = f"otel_etl is not importable: {e}"
        if output_json:
            console.print(json.dumps({"error": msg}))
        else:
            console.print(f"[red]Error:[/] {msg}")
        return 1

    prometheus_url = args.url
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=args.minutes)
    max_retries = getattr(args, "retries", 3)
    chunk_minutes = getattr(args, "chunk_minutes", CHUNK_MINUTES)

    try:
        if not output_json:
            console.print(f"Discovering metrics on [cyan]{prometheus_url}[/] ...")
        the_metrics = _discover_metrics(get_metrics_dataframe2, prometheus_url, max_retries)

        if not output_json:
            console.print(f"Found [bold]{len(the_metrics)}[/] metric series")

        client = PrometheusClient(prometheus_url)

        # Build time chunks
        chunks = []
        chunk_start = start_time
        while chunk_start < end_time:
            chunk_end = min(chunk_start + timedelta(minutes=chunk_minutes), end_time)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end

        all_dfs = []
        for i, (c_start, c_end) in enumerate(chunks, 1):
            chunk_df = _fetch_chunk(
                client, the_metrics, c_start, c_end, args.step, max_retries
            )
            all_dfs.append(chunk_df)
            if not output_json:
                mins = int((c_end - c_start).total_seconds() / 60)
                console.print(
                    f"  Chunk {i}/{len(chunks)}: fetching {mins} min "
                    f"({c_start.strftime('%H:%M')}–{c_end.strftime('%H:%M')}) ..."
                    f"  ({len(chunk_df)} tuples)"
                )

        raw_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
        raw_df.to_csv(args.outfile, index=False)

        if output_json:
            console.print(json.dumps({
                "file": args.outfile,
                "rows": len(raw_df),
                "metrics": len(the_metrics),
                "chunks": len(chunks),
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            }))
        else:
            console.print(
                f"[green]Saved [bold]{len(raw_df)}[/] rows to {args.outfile}[/]"
            )
        return 0
    except Exception as e:
        if output_json:
            console.print(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]Error:[/] {e}")
        return 1


# ---------------------------------------------------------------------------
# Jaeger trace fetching (gRPC via signals module)
# ---------------------------------------------------------------------------


def _parse_jaeger_url(jaeger_url: str) -> tuple[str, int]:
    """Parse host and port from a Jaeger URL like 'http://host:port' or 'host:port'."""
    url = jaeger_url.rstrip("/")
    if "://" in url:
        url = url.split("://", 1)[1]
    if ":" in url:
        host, port_str = url.rsplit(":", 1)
        return host, int(port_str)
    return url, 16686


def _jaeger_spans_to_csv_df(spans_df: pd.DataFrame) -> pd.DataFrame:
    """Convert a signals.fetch_traces() DataFrame to the CSV format otel_synth expects.

    Input columns (from signals): trace_id, span_id, parent_span_id,
        operation_name, service_name, start_time (datetime), duration_us,
        tags (dict), logs (list), warnings
    Output columns: trace_id, span_id, parent_span_id, operation_name,
        service_name, start_time (microseconds), duration_us, status_code,
        tags_json, logs_json
    """
    if spans_df.empty:
        return pd.DataFrame(columns=[
            "trace_id", "span_id", "parent_span_id", "operation_name",
            "service_name", "start_time", "duration_us", "status_code",
            "tags_json", "logs_json",
        ])

    rows = []
    for _, row in spans_df.iterrows():
        tags = row.get("tags", {}) or {}

        status_code = "OK"
        if str(tags.get("otel.status_code", "")).upper() == "ERROR":
            status_code = "ERROR"
        elif tags.get("error") in (True, "true", "1"):
            status_code = "ERROR"

        # Convert start_time datetime to microseconds
        st = row["start_time"]
        if isinstance(st, datetime):
            start_us = int(st.timestamp() * 1_000_000)
        else:
            start_us = int(st)

        # Convert logs list to JSON-serializable format
        logs_raw = row.get("logs", []) or []
        logs_json_list = []
        for log_entry in logs_raw:
            entry = {}
            if isinstance(log_entry, dict):
                ts = log_entry.get("timestamp")
                if isinstance(ts, datetime):
                    entry["timestamp_us"] = int(ts.timestamp() * 1_000_000)
                fields = log_entry.get("fields", {})
                entry["fields"] = {k: str(v) for k, v in fields.items()} if fields else {}
            logs_json_list.append(entry)

        rows.append({
            "trace_id": row["trace_id"],
            "span_id": row["span_id"],
            "parent_span_id": row.get("parent_span_id", "") or "",
            "operation_name": row["operation_name"],
            "service_name": row["service_name"],
            "start_time": start_us,
            "duration_us": row["duration_us"],
            "status_code": status_code,
            "tags_json": json.dumps({k: str(v) for k, v in tags.items()}),
            "logs_json": json.dumps(logs_json_list),
        })

    return pd.DataFrame(rows)


_JAEGER_MAX_TRACES = 2000


def _fetch_jaeger_traces(
    args: argparse.Namespace,
    console: Console,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame | None:
    """Fetch traces from Jaeger via gRPC (using signals module). Returns None if no Jaeger URL.

    Fetches in time chunks (same --chunk-minutes as metrics) and warns if any chunk
    hits the max_traces cap (indicating the window may be too wide to capture all data).
    """
    jaeger_url = getattr(args, "jaeger_url", None)
    if not jaeger_url:
        return None

    output_json = getattr(args, "output_format", "text") == "json"
    chunk_minutes = getattr(args, "chunk_minutes", CHUNK_MINUTES)

    try:
        from signals.traces.jaeger import list_services, fetch_traces
    except ImportError as e:
        msg = f"signals.traces.jaeger is not importable: {e}"
        if output_json:
            console.print(json.dumps({"jaeger_error": msg}))
        else:
            console.print(f"[red]Jaeger error:[/] {msg}")
        return None

    host, port = _parse_jaeger_url(jaeger_url)

    try:
        if not output_json:
            console.print(f"Discovering services on Jaeger [cyan]{host}:{port}[/] (gRPC) ...")
        services = list_services(host=host, port=port)
        if not services:
            raise RuntimeError("Jaeger returned no services")
        if not output_json:
            console.print(f"Found [bold]{len(services)}[/] services on Jaeger")

        # Build time chunks (same logic as metrics)
        chunks = []
        chunk_start = start_time
        while chunk_start < end_time:
            chunk_end = min(chunk_start + timedelta(minutes=chunk_minutes), end_time)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end

        all_dfs = []
        for i, (c_start, c_end) in enumerate(chunks, 1):
            chunk_spans = 0
            for svc in services:
                svc_df = fetch_traces(
                    service_name=svc,
                    start_time=c_start,
                    end_time=c_end,
                    max_traces=_JAEGER_MAX_TRACES,
                    host=host,
                    port=port,
                )
                if svc_df.empty:
                    continue
                all_dfs.append(svc_df)
                chunk_spans += len(svc_df)
                n_traces = svc_df["trace_id"].nunique()
                if n_traces >= _JAEGER_MAX_TRACES:
                    warn = (
                        f"WARNING: service '{svc}' returned {n_traces} traces in chunk "
                        f"{c_start.strftime('%H:%M')}–{c_end.strftime('%H:%M')} "
                        f"(hit cap of {_JAEGER_MAX_TRACES}) — data may be truncated; "
                        f"consider reducing --chunk-minutes"
                    )
                    if output_json:
                        console.print(json.dumps({"jaeger_warning": warn}))
                    else:
                        console.print(f"[yellow]⚠ {warn}[/]")
            if not output_json:
                console.print(
                    f"  Chunk {i}/{len(chunks)}: traces "
                    f"({c_start.strftime('%H:%M')}–{c_end.strftime('%H:%M')}) ..."
                    f"  ({chunk_spans} spans)"
                )

        if not all_dfs:
            if not output_json:
                console.print("[yellow]No traces found[/]")
            return pd.DataFrame()

        combined = pd.concat(all_dfs, ignore_index=True)

        # Deduplicate spans (same span may appear via different service/chunk queries)
        combined = combined.drop_duplicates(subset=["trace_id", "span_id"])

        # Convert to CSV format
        csv_df = _jaeger_spans_to_csv_df(combined)

        if not output_json:
            n_traces = csv_df["trace_id"].nunique()
            console.print(
                f"[green]Fetched [bold]{len(csv_df)}[/] spans "
                f"from {n_traces} unique traces[/]"
            )
        return csv_df

    except Exception as e:
        if output_json:
            console.print(json.dumps({"jaeger_error": str(e)}))
        else:
            console.print(f"[red]Jaeger error:[/] {e}")
        return None


# ---------------------------------------------------------------------------
# OpenSearch log fetching
# ---------------------------------------------------------------------------


def _opensearch_fetch_logs(
    opensearch_url: str,
    start_time: datetime,
    end_time: datetime,
    max_retries: int,
    batch_size: int = 5000,
) -> pd.DataFrame:
    """Fetch logs from OpenSearch for a time range.

    Queries the otel-logs-* index pattern used by the OTel demo collector.
    """
    import httpx

    # ISO format for OpenSearch range queries
    start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_iso = end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    query = {
        "size": batch_size,
        "sort": [{"@timestamp": {"order": "asc"}}],
        "query": {
            "range": {
                "@timestamp": {
                    "gte": start_iso,
                    "lte": end_iso,
                }
            }
        },
    }

    all_rows = []
    search_after = None

    while True:
        body = dict(query)
        if search_after is not None:
            body["search_after"] = search_after

        fetched = False
        for attempt in range(1, max_retries + 1):
            try:
                resp = httpx.post(
                    f"{opensearch_url}/otel-logs-*/_search",
                    json=body,
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                fetched = True
                break
            except Exception:
                if attempt < max_retries:
                    time.sleep(5 * attempt)
                    continue
                raise

        if not fetched:
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            src = hit.get("_source", {})
            # Extract fields matching the expected CSV format
            body_val = src.get("body", src.get("Body", ""))
            if isinstance(body_val, dict):
                body_val = body_val.get("stringValue", str(body_val))

            severity = src.get("severity", src.get("SeverityText", src.get("severityText", "INFO")))
            service = (
                src.get("service_name")
                or src.get("ServiceName")
                or _extract_resource_attr(src, "service.name")
                or "unknown"
            )
            trace_id = src.get("traceId", src.get("TraceId", src.get("trace_id", "")))
            span_id = src.get("spanId", src.get("SpanId", src.get("span_id", "")))
            timestamp = src.get("@timestamp", src.get("observedTimestamp", src.get("Timestamp", "")))

            all_rows.append({
                "timestamp": timestamp,
                "service": service,
                "severity": severity,
                "message": body_val,
                "trace_id": trace_id,
                "span_id": span_id,
            })

        # Pagination: use search_after from last hit's sort value
        last_sort = hits[-1].get("sort")
        if last_sort and len(hits) == batch_size:
            search_after = last_sort
        else:
            break

    if not all_rows:
        return pd.DataFrame(columns=["timestamp", "service", "severity", "message", "trace_id", "span_id"])

    return pd.DataFrame(all_rows)


def _extract_resource_attr(src: dict, key: str) -> str:
    """Extract a resource attribute from OpenSearch log source doc."""
    # OTel logs in OpenSearch may nest resource attributes differently
    resource = src.get("resource", {})
    if isinstance(resource, dict):
        attrs = resource.get("attributes", resource)
        if isinstance(attrs, dict):
            return attrs.get(key, "")
        if isinstance(attrs, list):
            for a in attrs:
                if isinstance(a, dict) and a.get("key") == key:
                    val = a.get("value", {})
                    if isinstance(val, dict):
                        return val.get("stringValue", str(val))
                    return str(val)
    return ""


def _fetch_opensearch_logs(
    args: argparse.Namespace,
    console: Console,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame | None:
    """Fetch logs from OpenSearch and return as DataFrame. Returns None if no OpenSearch URL.

    Fetches in time chunks (same --chunk-minutes as metrics) to avoid overwhelming OpenSearch.
    Within each chunk, search_after pagination ensures all records are retrieved.
    """
    opensearch_url = getattr(args, "opensearch_url", None)
    if not opensearch_url:
        return None

    output_json = getattr(args, "output_format", "text") == "json"
    max_retries = getattr(args, "retries", 3)
    chunk_minutes = getattr(args, "chunk_minutes", CHUNK_MINUTES)
    opensearch_url = opensearch_url.rstrip("/")

    # Build time chunks
    chunks = []
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + timedelta(minutes=chunk_minutes), end_time)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end

    try:
        if not output_json:
            console.print(f"Fetching logs from OpenSearch [cyan]{opensearch_url}[/] ...")

        all_dfs = []
        for i, (c_start, c_end) in enumerate(chunks, 1):
            chunk_df = _opensearch_fetch_logs(opensearch_url, c_start, c_end, max_retries)
            if not chunk_df.empty:
                all_dfs.append(chunk_df)
            if not output_json:
                console.print(
                    f"  Chunk {i}/{len(chunks)}: logs "
                    f"({c_start.strftime('%H:%M')}–{c_end.strftime('%H:%M')}) ..."
                    f"  ({len(chunk_df)} records)"
                )

        df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(
            columns=["timestamp", "service", "severity", "message", "trace_id", "span_id"]
        )

        if not output_json:
            n_services = df["service"].nunique() if not df.empty else 0
            console.print(
                f"[green]Fetched [bold]{len(df)}[/] log records "
                f"from {n_services} services[/]"
            )
        return df
    except Exception as e:
        if output_json:
            console.print(json.dumps({"opensearch_error": str(e)}))
        else:
            console.print(f"[red]OpenSearch error:[/] {e}")
        return None


# ---------------------------------------------------------------------------
# Output path helpers
# ---------------------------------------------------------------------------


def _traces_outfile(outfile: str) -> str:
    """Derive traces CSV path from metrics outfile. foo.csv -> foo_traces.csv"""
    p = Path(outfile)
    return str(p.with_stem(p.stem + "_traces"))


def _logs_outfile(outfile: str) -> str:
    """Derive logs CSV path from metrics outfile. foo.csv -> foo_logs.csv"""
    p = Path(outfile)
    return str(p.with_stem(p.stem + "_logs"))


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run(args: argparse.Namespace, console: Console) -> int:
    output_json = getattr(args, "output_format", "text") == "json"

    # Compute time range (shared by all fetchers)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=args.minutes)

    # 1. Fetch metrics
    if getattr(args, "use_dd", False):
        code = _run_dd(args, console)
    else:
        code = _run_prometheus(args, console)

    if code != 0:
        return code

    # 2. Fetch traces (if --jaeger-url provided)
    traces_df = _fetch_jaeger_traces(args, console, start_time, end_time)
    if traces_df is not None and not traces_df.empty:
        traces_path = _traces_outfile(args.outfile)
        traces_df.to_csv(traces_path, index=False)
        if output_json:
            console.print(json.dumps({"traces_file": traces_path, "traces_rows": len(traces_df)}))
        else:
            console.print(f"[green]Saved [bold]{len(traces_df)}[/] spans to {traces_path}[/]")

    # 3. Fetch logs (if --opensearch-url provided)
    logs_df = _fetch_opensearch_logs(args, console, start_time, end_time)
    if logs_df is not None and not logs_df.empty:
        logs_path = _logs_outfile(args.outfile)
        logs_df.to_csv(logs_path, index=False)
        if output_json:
            console.print(json.dumps({"logs_file": logs_path, "logs_rows": len(logs_df)}))
        else:
            console.print(f"[green]Saved [bold]{len(logs_df)}[/] log records to {logs_path}[/]")

    return 0
