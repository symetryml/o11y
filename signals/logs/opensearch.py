#!/usr/bin/env python3
"""
Fetch logs from OpenSearch.

Usage in notebook:
    from python_src.s006_logs import fetch_logs, search_logs

    # Fetch all logs from the last hour
    df = fetch_logs()

    # Fetch logs for specific services
    df = fetch_logs(services=['frontend', 'checkout'])

    # Fetch logs with severity level filter
    df = fetch_logs(severity=['info', 'warn', 'error'])

    # Search logs with custom query
    df = search_logs(
        query_string="exception OR error",
        services=['frontend'],
        time_range_minutes=60
    )
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional


def fetch_logs(
    opensearch_url: str = "http://localhost:9200",
    services: Optional[List[str]] = None,
    severity: Optional[List[str]] = None,
    time_range_minutes: int = 60,
    max_results: int = 1000
) -> pd.DataFrame:
    """
    Fetch logs from OpenSearch.

    Args:
        opensearch_url: OpenSearch server URL
        services: List of service names to filter (e.g., ['frontend', 'checkout'])
        severity: List of severity levels to filter (e.g., ['error', 'warn', 'info'])
                  Note: use lowercase
        time_range_minutes: How far back to fetch logs (default: 60 minutes)
        max_results: Maximum number of log entries to return (default: 1000)

    Returns:
        DataFrame with columns:
        ['timestamp', 'service', 'severity', 'severity_number', 'message',
         'trace_id', 'span_id', 'attributes', 'resource', 'raw']
    """
    # Build the query
    must_clauses = []

    # Time range filter - use @timestamp
    must_clauses.append({
        "range": {
            "@timestamp": {
                "gte": f"now-{time_range_minutes}m",
                "lte": "now"
            }
        }
    })

    # Service filter
    if services:
        must_clauses.append({
            "terms": {
                "resource.service.name": services
            }
        })

    # Severity filter
    if severity:
        must_clauses.append({
            "terms": {
                "severity.text": severity
            }
        })

    query = {
        "size": max_results,
        "query": {
            "bool": {
                "must": must_clauses
            }
        },
        "sort": [
            {"@timestamp": {"order": "desc"}}
        ]
    }

    # Query OpenSearch - search across all otel-logs indices
    url = f"{opensearch_url.rstrip('/')}/otel-logs-*/_search"
    response = requests.post(url, json=query, headers={"Content-Type": "application/json"})

    if response.status_code != 200:
        print(f"Error querying OpenSearch: {response.status_code}")
        print(f"Response: {response.text}")
        return pd.DataFrame(columns=[
            'timestamp', 'service', 'severity', 'severity_number', 'message',
            'trace_id', 'span_id', 'attributes', 'resource', 'raw'
        ])

    result = response.json()

    # Parse the results
    data = []
    for hit in result.get('hits', {}).get('hits', []):
        source = hit['_source']

        # Extract timestamp
        timestamp_str = source.get('@timestamp', source.get('observedTimestamp', ''))
        try:
            # Parse ISO format timestamp
            timestamp = pd.to_datetime(timestamp_str)
        except (ValueError, TypeError):
            timestamp = None

        # Extract service name from resource
        resource = source.get('resource', {})
        service = resource.get('service.name', 'unknown')

        # Extract severity
        severity_obj = source.get('severity', {})
        severity_text = severity_obj.get('text', 'unknown')
        severity_number = severity_obj.get('number', 0)

        # Extract log message
        message = source.get('body', '')

        # Extract trace/span IDs - check top level first, then attributes
        attributes = source.get('attributes', {})
        trace_id = source.get('traceId') or source.get('trace_id') or attributes.get('traceId') or attributes.get('trace_id') or ''
        span_id = source.get('spanId') or source.get('span_id') or attributes.get('spanId') or attributes.get('span_id') or ''

        data.append({
            'timestamp': timestamp,
            'service': service,
            'severity': severity_text,
            'severity_number': severity_number,
            'message': message,
            'trace_id': trace_id,
            'span_id': span_id,
            'attributes': attributes,
            'resource': resource,
            'raw': source
        })

    df = pd.DataFrame(data)
    return df


def search_logs(
    query_string: str,
    opensearch_url: str = "http://localhost:9200",
    services: Optional[List[str]] = None,
    time_range_minutes: int = 60,
    max_results: int = 1000
) -> pd.DataFrame:
    """
    Search logs using query string syntax.

    Args:
        query_string: Query string (e.g., "exception OR error", "status:500")
        opensearch_url: OpenSearch server URL
        services: Optional list of service names to filter
        time_range_minutes: How far back to search (default: 60 minutes)
        max_results: Maximum number of results (default: 1000)

    Returns:
        DataFrame with log entries matching the query
    """
    must_clauses = [
        {
            "range": {
                "@timestamp": {
                    "gte": f"now-{time_range_minutes}m",
                    "lte": "now"
                }
            }
        },
        {
            "query_string": {
                "query": query_string,
                "default_field": "body"
            }
        }
    ]

    if services:
        must_clauses.append({
            "terms": {
                "resource.service.name": services
            }
        })

    query = {
        "size": max_results,
        "query": {
            "bool": {
                "must": must_clauses
            }
        },
        "sort": [
            {"@timestamp": {"order": "desc"}}
        ]
    }

    url = f"{opensearch_url.rstrip('/')}/otel-logs-*/_search"
    response = requests.post(url, json=query, headers={"Content-Type": "application/json"})

    if response.status_code != 200:
        print(f"Error querying OpenSearch: {response.status_code}")
        print(f"Response: {response.text}")
        return pd.DataFrame(columns=[
            'timestamp', 'service', 'severity', 'severity_number', 'message',
            'trace_id', 'span_id', 'attributes', 'resource', 'raw'
        ])

    result = response.json()

    # Parse results (same as fetch_logs)
    data = []
    for hit in result.get('hits', {}).get('hits', []):
        source = hit['_source']

        timestamp_str = source.get('@timestamp', source.get('observedTimestamp', ''))
        try:
            timestamp = pd.to_datetime(timestamp_str)
        except (ValueError, TypeError):
            timestamp = None

        resource = source.get('resource', {})
        service = resource.get('service.name', 'unknown')

        severity_obj = source.get('severity', {})
        severity_text = severity_obj.get('text', 'unknown')
        severity_number = severity_obj.get('number', 0)

        message = source.get('body', '')

        # Extract trace/span IDs - check top level first, then attributes
        attributes = source.get('attributes', {})
        trace_id = source.get('traceId') or source.get('trace_id') or attributes.get('traceId') or attributes.get('trace_id') or ''
        span_id = source.get('spanId') or source.get('span_id') or attributes.get('spanId') or attributes.get('span_id') or ''

        data.append({
            'timestamp': timestamp,
            'service': service,
            'severity': severity_text,
            'severity_number': severity_number,
            'message': message,
            'trace_id': trace_id,
            'span_id': span_id,
            'attributes': attributes,
            'resource': resource,
            'raw': source
        })

    df = pd.DataFrame(data)
    return df


def get_log_statistics(
    opensearch_url: str = "http://localhost:9200",
    time_range_minutes: int = 60
) -> pd.DataFrame:
    """
    Get aggregated statistics about logs by service and severity.

    Args:
        opensearch_url: OpenSearch server URL
        time_range_minutes: Time range to analyze (default: 60 minutes)

    Returns:
        DataFrame with log counts grouped by service and severity
    """
    query = {
        "size": 0,
        "query": {
            "range": {
                "@timestamp": {
                    "gte": f"now-{time_range_minutes}m",
                    "lte": "now"
                }
            }
        },
        "aggs": {
            "by_service": {
                "terms": {
                    "field": "resource.service.name.keyword",
                    "size": 100
                },
                "aggs": {
                    "by_severity": {
                        "terms": {
                            "field": "severity.text.keyword",
                            "size": 20
                        }
                    }
                }
            }
        }
    }

    url = f"{opensearch_url.rstrip('/')}/otel-logs-*/_search"
    response = requests.post(url, json=query, headers={"Content-Type": "application/json"})

    if response.status_code != 200:
        print(f"Error querying OpenSearch: {response.status_code}")
        print(f"Response: {response.text}")
        return pd.DataFrame(columns=['service', 'severity', 'count'])

    result = response.json()

    # Parse aggregations
    data = []
    for service_bucket in result.get('aggregations', {}).get('by_service', {}).get('buckets', []):
        service_name = service_bucket['key']

        for severity_bucket in service_bucket.get('by_severity', {}).get('buckets', []):
            severity = severity_bucket['key']
            count = severity_bucket['doc_count']

            data.append({
                'service': service_name,
                'severity': severity,
                'count': count
            })

    df = pd.DataFrame(data)
    return df


if __name__ == "__main__":
    # Example usage
    print("Fetching recent logs...")
    df = fetch_logs(time_range_minutes=10, max_results=20)
    print(f"\nFound {len(df)} log entries")

    if len(df) > 0:
        print("\nSample logs:")
        print(df[['timestamp', 'service', 'severity', 'message']].head(10))

        print("\n\nLog statistics:")
        stats = get_log_statistics(time_range_minutes=10)
        print(stats)
