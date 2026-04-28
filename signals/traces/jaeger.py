#!/usr/bin/env python3
"""
Fetch trace data from Jaeger gRPC API with actual trace IDs.

Usage in notebook:
    from signals import (
        list_services,
        list_operations,
        fetch_traces,
        get_trace_by_id,
        aggregate_spans_to_traces
    )

    # List services
    services = list_services()

    # Fetch traces with trace_ids
    df = fetch_traces(service_name="frontend", max_traces=100)

    # Group by trace_id
    for trace_id, spans in df.groupby('trace_id'):
        print(f"Trace {trace_id}: {len(spans)} spans")

    # Aggregate to trace level
    traces = aggregate_spans_to_traces(df)
"""

import os as _os
import sys as _sys

# Ensure _proto directory is on sys.path for bare protobuf imports
_proto_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "_proto")
if _proto_dir not in _sys.path:
    _sys.path.insert(0, _proto_dir)

import grpc
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import warnings

import query_pb2
import query_pb2_grpc
import model_pb2


def list_services(
    host: str = "localhost",
    port: int = 16685
) -> List[str]:
    """
    List all services from Jaeger.

    Args:
        host: Jaeger gRPC host
        port: Jaeger gRPC port (default: 59767)

    Returns:
        List of service names
    """
    try:
        channel = grpc.insecure_channel(f'{host}:{port}')
        stub = query_pb2_grpc.QueryServiceStub(channel)
        request = query_pb2.GetServicesRequest()
        response = stub.GetServices(request, timeout=10)
        services = list(response.services)
        channel.close()
        return sorted(services)
    except Exception as e:
        warnings.warn(f"Failed to fetch services: {e}")
        return []


def list_operations(
    service_name: str,
    host: str = "localhost",
    port: int = 16685
) -> List[str]:
    """
    List operations for a service.

    Args:
        service_name: Service name
        host: Jaeger gRPC host
        port: Jaeger gRPC port

    Returns:
        List of operation names
    """
    try:
        channel = grpc.insecure_channel(f'{host}:{port}')
        stub = query_pb2_grpc.QueryServiceStub(channel)
        request = query_pb2.GetOperationsRequest(service=service_name)
        response = stub.GetOperations(request, timeout=10)
        operations = [op.name for op in response.operations]
        channel.close()
        return sorted(operations)
    except Exception as e:
        warnings.warn(f"Failed to fetch operations: {e}")
        return []


def fetch_traces(
    service_name: str,
    operation_name: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    min_duration_ms: Optional[float] = None,
    max_duration_ms: Optional[float] = None,
    tags: Optional[Dict[str, str]] = None,
    max_traces: int = 100,
    host: str = "localhost",
    port: int = 16685
) -> pd.DataFrame:
    """
    Fetch traces with trace_ids from Jaeger.

    Args:
        service_name: Service to query
        operation_name: Optional operation filter
        start_time: Start time
        end_time: End time
        min_duration_ms: Min duration in milliseconds
        max_duration_ms: Max duration in milliseconds
        tags: Optional tag filters
        max_traces: Max traces to fetch
        host: Jaeger host
        port: Jaeger port

    Returns:
        DataFrame with columns: trace_id, span_id, parent_span_id,
                                operation_name, service_name, start_time,
                                duration_us, tags, logs, warnings
    """
    channel = grpc.insecure_channel(f'{host}:{port}')
    stub = query_pb2_grpc.QueryServiceStub(channel)

    # Build query
    query = query_pb2.TraceQueryParameters(
        service_name=service_name,
        search_depth=max_traces
    )

    if operation_name:
        query.operation_name = operation_name

    # Time range - only set if explicitly provided
    if start_time:
        query.start_time_min.FromDatetime(start_time)
    if end_time:
        query.start_time_max.FromDatetime(end_time)

    # Duration filters
    if min_duration_ms:
        query.duration_min.FromMicroseconds(int(min_duration_ms * 1000))
    if max_duration_ms:
        query.duration_max.FromMicroseconds(int(max_duration_ms * 1000))

    # Tag filters
    if tags:
        for key, value in tags.items():
            query.tags[key] = value

    # Execute query
    request = query_pb2.FindTracesRequest(query=query)
    response_stream = stub.FindTraces(request, timeout=30)

    all_spans = []

    for chunk in response_stream:
        for span in chunk.spans:
            # Trace and span IDs
            trace_id = span.trace_id.hex()
            span_id = span.span_id.hex()

            # Parent span ID
            parent_span_id = None
            for ref in span.references:
                if ref.ref_type == model_pb2.SpanRefType.CHILD_OF:
                    parent_span_id = ref.span_id.hex()
                    break

            # Service name
            service_name_actual = span.process.service_name if span.process else service_name

            # Tags
            tags_dict = {}
            for tag in span.tags:
                tags_dict[tag.key] = _extract_tag_value(tag)

            # Logs
            logs = []
            for log in span.logs:
                log_entry = {
                    'timestamp': datetime.fromtimestamp(log.timestamp.seconds + log.timestamp.nanos / 1e9),
                    'fields': {field.key: _extract_tag_value(field) for field in log.fields}
                }
                logs.append(log_entry)

            all_spans.append({
                'trace_id': trace_id,
                'span_id': span_id,
                'parent_span_id': parent_span_id,
                'operation_name': span.operation_name,
                'service_name': service_name_actual,
                'start_time': datetime.fromtimestamp(span.start_time.seconds + span.start_time.nanos / 1e9),
                'duration_us': span.duration.ToMicroseconds(),
                'tags': tags_dict,
                'logs': logs,
                'warnings': list(span.warnings) if span.warnings else []
            })

    channel.close()

    if not all_spans:
        return pd.DataFrame(columns=[
            'trace_id', 'span_id', 'parent_span_id', 'operation_name',
            'service_name', 'start_time', 'duration_us', 'tags', 'logs', 'warnings'
        ])

    return pd.DataFrame(all_spans)


def get_trace_by_id(
    trace_id: str,
    host: str = "localhost",
    port: int = 16685
) -> pd.DataFrame:
    """
    Fetch a specific trace by ID.

    Args:
        trace_id: Trace ID (hex string)
        host: Jaeger host
        port: Jaeger port

    Returns:
        DataFrame with all spans in the trace
    """
    channel = grpc.insecure_channel(f'{host}:{port}')
    stub = query_pb2_grpc.QueryServiceStub(channel)

    trace_id_bytes = bytes.fromhex(trace_id)
    request = query_pb2.GetTraceRequest(trace_id=trace_id_bytes)
    response_stream = stub.GetTrace(request, timeout=10)

    all_spans = []

    for chunk in response_stream:
        for span in chunk.spans:
            trace_id = span.trace_id.hex()
            span_id = span.span_id.hex()

            parent_span_id = None
            for ref in span.references:
                if ref.ref_type == model_pb2.SpanRefType.CHILD_OF:
                    parent_span_id = ref.span_id.hex()
                    break

            service_name = span.process.service_name if span.process else "unknown"

            tags_dict = {}
            for tag in span.tags:
                tags_dict[tag.key] = _extract_tag_value(tag)

            logs = []
            for log in span.logs:
                log_entry = {
                    'timestamp': datetime.fromtimestamp(log.timestamp.seconds + log.timestamp.nanos / 1e9),
                    'fields': {field.key: _extract_tag_value(field) for field in log.fields}
                }
                logs.append(log_entry)

            all_spans.append({
                'trace_id': trace_id,
                'span_id': span_id,
                'parent_span_id': parent_span_id,
                'operation_name': span.operation_name,
                'service_name': service_name,
                'start_time': datetime.fromtimestamp(span.start_time.seconds + span.start_time.nanos / 1e9),
                'duration_us': span.duration.ToMicroseconds(),
                'tags': tags_dict,
                'logs': logs,
                'warnings': list(span.warnings) if span.warnings else []
            })

    channel.close()
    return pd.DataFrame(all_spans)


def aggregate_spans_to_traces(spans_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate spans to trace-level data.

    Args:
        spans_df: DataFrame from fetch_traces()

    Returns:
        DataFrame with one row per trace
    """
    if spans_df.empty:
        return pd.DataFrame(columns=[
            'trace_id', 'root_service', 'root_operation', 'start_time',
            'total_duration_ms', 'num_spans', 'num_services', 'has_error'
        ])

    trace_data = []

    for trace_id, group in spans_df.groupby('trace_id'):
        # Find root span (no parent)
        root_spans = group[group['parent_span_id'].isna()]
        root_span = root_spans.iloc[0] if not root_spans.empty else group.iloc[0]

        # Check for errors
        has_error = False
        for tags in group['tags']:
            if isinstance(tags, dict):
                if tags.get('error') in [True, 'true', '1']:
                    has_error = True
                    break
                if tags.get('otel.status_code') == 'ERROR':
                    has_error = True
                    break

        trace_data.append({
            'trace_id': trace_id,
            'root_service': root_span['service_name'],
            'root_operation': root_span['operation_name'],
            'start_time': group['start_time'].min(),
            'total_duration_ms': group['duration_us'].sum() / 1000,
            'num_spans': len(group),
            'num_services': group['service_name'].nunique(),
            'has_error': has_error
        })

    return pd.DataFrame(trace_data)


def _extract_tag_value(tag):
    """Extract value from Jaeger KeyValue tag."""
    if tag.v_type == model_pb2.ValueType.STRING:
        return tag.v_str
    elif tag.v_type == model_pb2.ValueType.BOOL:
        return tag.v_bool
    elif tag.v_type == model_pb2.ValueType.INT64:
        return tag.v_int64
    elif tag.v_type == model_pb2.ValueType.FLOAT64:
        return tag.v_float64
    elif tag.v_type == model_pb2.ValueType.BINARY:
        return tag.v_binary
    else:
        return None


# Example usage
if __name__ == "__main__":
    print("=" * 80)
    print("Fetching Traces from Jaeger with Trace IDs")
    print("=" * 80)

    # List services
    print("\n1. Services:")
    services = list_services()
    for svc in services[:10]:
        print(f"   - {svc}")

    # Fetch traces
    print("\n2. Fetching traces for 'frontend':")
    df = fetch_traces(service_name="frontend", max_traces=10)
    print(f"   Fetched {len(df)} spans")

    if not df.empty:
        print("\n   Sample:")
        print(df[['trace_id', 'operation_name', 'service_name', 'duration_us']].head(10))

        print("\n3. Group by trace_id:")
        for trace_id, spans in df.groupby('trace_id'):
            print(f"   {trace_id[:16]}...: {len(spans)} spans, "
                  f"{spans['service_name'].nunique()} services, "
                  f"{spans['duration_us'].sum()/1000:.2f}ms")

        print("\n4. Aggregated traces:")
        traces = aggregate_spans_to_traces(df)
        print(traces.to_string(index=False))
