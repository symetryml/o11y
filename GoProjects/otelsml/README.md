# otelsml

A custom OpenTelemetry Collector and CLI for transforming raw OTel metrics into ML-ready feature DataFrames for [SymetryML](https://symetryml.com).

It implements the `denormalize_metrics` pipeline in Go: raw metrics go in, wide-format feature tables come out — with aggregation, status bucketing, histogram percentiles, entity keys, and delta/pct-change features.

## Architecture

```
                          +-----------------+
                          | Stock OTel      |
  Services ──OTLP──────> | Collector       |──OTLP──> Prometheus / Jaeger
                          | (port 4317/18)  |
                          +-------+---------+
                                  |
                                  | OTLP (forwarded)
                                  v
                          +-----------------+
                          | otelsml         |
                          | (port 4319/20)  |
                          |                 |
                          |  smlprocessor   |  OTLP → DataFrame → denormalize_metrics → OTLP
                          |  smlexporter    |  Output: JSON / CSV / DEM API
                          +-----------------+
```

otelsml chains after a stock OTel Collector. The stock collector handles standard telemetry routing (Prometheus, Jaeger, etc.), then forwards metrics via OTLP to otelsml for ML feature extraction.

## Components

### smlprocessor

Receives OTLP metrics, converts them to a [godf](../godf) DataFrame, runs the full `denormalize_metrics` pipeline from [oteletl](../oteletl), and emits wide-format features as new OTel gauge metrics.

**Pipeline stages:**

1. OTLP to DataFrame (Gauge, Sum/Counter, Histogram with bucket expansion)
2. Filter by service (`pipeline.FilterByService`)
3. Transform labels (status bucketing, HTTP method, route parameterization)
4. Build entity keys from label combinations
5. Aggregate per entity/timestamp (gauge stats, counter rates, histogram percentiles)
6. Pivot to wide format (one row per timestamp/entity, features as columns)
7. Compute deltas and pct-change features

### smlexporter

Outputs the processed metrics in one of three formats:

| Format | Description |
|--------|-------------|
| `json` | One JSON line per metric data point (metric name, timestamp, value, entity_key) |
| `csv` | Wide-format CSV (one row per timestamp/entity, all features as columns) |
| `dem` | Stream to SymetryML DEM API via [demclient](../demclient) |

## Configuration

### Collector YAML

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4319
      http:
        endpoint: 0.0.0.0:4320

processors:
  smlprocessor:
    services: [checkout]          # filter to specific services (empty = all)
    service_label: service_name   # label used to identify the service
    schema_path: /etc/schema.yaml # optional schema file for label bucketing
    window_seconds: 60            # aggregation window
    include_deltas: true          # compute delta/pct-change features
    delta_windows: [5, 60]        # shift amounts for delta features
    entity_labels: [service_name] # labels that form the entity key
    force_drop_labels: []         # labels to always remove

exporters:
  smlexporter:
    format: json                  # json, csv
    output_path: stdout           # stdout or file path
    dem_endpoint: http://dem:8080 # SymetryML DEM server (optional)
    project_name: my-project      # DEM project name (optional)

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [smlprocessor]
      exporters: [smlexporter]
```

### Environment Variables

Every processor config field can be overridden by an environment variable. Env vars take precedence over YAML config.

#### Processor (smlprocessor)

| Variable | Description | Default |
|----------|-------------|---------|
| `SML_SERVICES` | Comma-separated service filter (empty = process all) | _(all)_ |
| `SML_SERVICE_LABEL` | Label name for service identification | `service_name` |
| `SML_SCHEMA_PATH` | Path to schema YAML file | _(none)_ |
| `SML_WINDOW_SECONDS` | Aggregation window in seconds | `60` |
| `SML_INCLUDE_DELTAS` | Enable delta/pct-change features (`true`/`false`) | `true` |
| `SML_ENTITY_LABELS` | Comma-separated entity key labels | _(auto-detect)_ |
| `SML_FORCE_DROP_LABELS` | Comma-separated labels to always drop | _(none)_ |

#### Exporter (smlexporter) — DEM streaming

| Variable | Description | Required |
|----------|-------------|----------|
| `SML_SERVER` | SymetryML DEM server URL | Yes (for DEM output) |
| `SML_KEY_ID` | DEM API key ID | Yes (for DEM output) |
| `SML_SECRET_KEY` | DEM API secret key (base64) | Yes (for DEM output) |
| `SML_PROJECT_NAME` | DEM project name | Yes (for DEM output) |

### Docker Compose

(Please also look at `../GoProjects/docker-compose-opentelemetry-demo` for example of configuration that you can
drop in into an existing `opentelemetry-demo`)
In the opentelemetry-demo docker-compose setup, the stock collector forwards OTLP to otelsml:

```yaml
otelsml:
  build:
    context: ../o11y/GoProjects
    dockerfile: otelsml/Dockerfile
  ports:
    - "4319:4319"
    - "4320:4320"
  depends_on:
    otel-collector:
      condition: service_started
  environment:
    - SML_SERVER=${SML_SERVER:?SML_SERVER must be set}
    - SML_KEY_ID=${SML_KEY_ID:?SML_KEY_ID must be set}
    - SML_SECRET_KEY=${SML_SECRET_KEY:?SML_SECRET_KEY must be set}
    - SML_PROJECT_NAME=${SML_PROJECT_NAME:?SML_PROJECT_NAME must be set}
    - SML_SERVICES
    - SML_SERVICE_LABEL
    - SML_SCHEMA_PATH
    - SML_WINDOW_SECONDS=60
    - SML_INCLUDE_DELTAS=true
    - SML_ENTITY_LABELS=
    - SML_FORCE_DROP_LABELS=
```

Required variables (`SML_SERVER`, `SML_KEY_ID`, `SML_SECRET_KEY`, `SML_PROJECT_NAME`) use the `${VAR:?error}` syntax and will cause `docker compose up` to fail immediately if unset.

## Building

```bash
cd GoProjects/otelsml
go build ./cmd/otelsml/       # OTel Collector binary
go build ./cmd/otelsmlcli/    # Standalone CLI
```

---

# otelsmlcli

Standalone CLI that runs the same denormalize_metrics pipeline without Docker or the OTel Collector framework. Connects directly to Prometheus or reads raw CSV files.

This replicates the Python workflow:

```python
df_metrics = get_metrics_dataframe2(prometheus_url)
the_service_metrics = df_metrics[df_metrics['service'] == the_service]['metric']
the_metrics = filter_salient_metrics(the_service_metrics)

raw_df = filter_by_service(df0, [the_service])
for window_start, window_end, window_df in iter_metrics_windows(raw_df, the_metrics, window_minutes=5, step="60s"):
    features_df = denormalize_metrics(window_df, schema_config=schema,
                                       entity_labels=["service_name"],
                                       window_seconds=60, include_deltas=True)
```

## Commands

### run

Full pipeline: fetch/load data, discover metrics, filter by service, window, denormalize, output.

```bash
# From live Prometheus
otelsmlcli run -prometheus-url http://localhost:9090 -service checkout

# From raw CSV file
otelsmlcli run -input-file /path/to/s001-all.csv -service checkout

# CSV output
otelsmlcli run -input-file data.csv -service checkout -output csv

# Stream to DEM
otelsmlcli run -prometheus-url http://localhost:9090 -service checkout -output dem
```

### discover

List available services and their metrics from Prometheus.

```bash
otelsmlcli discover -prometheus-url http://localhost:9090
```

### profile

Run the metric profiler and generate a schema YAML file.

```bash
otelsmlcli profile -prometheus-url http://localhost:9090 -output-path schema.yaml
otelsmlcli profile -input-file data.csv -service checkout -output-path schema.yaml
```

## Flags

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `-prometheus-url` | `SML_PROMETHEUS_URL` | | Prometheus base URL |
| `-input-file` | | | Raw CSV file path (alternative to Prometheus) |
| `-service` | `SML_SERVICES` | _(all)_ | Service to process |
| `-service-label` | `SML_SERVICE_LABEL` | `service_name` | Label key for service identification |
| `-schema-path` | `SML_SCHEMA_PATH` | | Schema YAML file for label bucketing |
| `-window-seconds` | `SML_WINDOW_SECONDS` | `60` | Aggregation window in seconds |
| `-window-minutes` | | `5` | Lookback window for `iter_metrics_windows` |
| `-step` | | `60s` | Resampling step size |
| `-start` | | _(now - window-minutes)_ | Range start (RFC3339) |
| `-end` | | _(now)_ | Range end (RFC3339) |
| `-include-deltas` | `SML_INCLUDE_DELTAS` | `true` | Compute delta/pct-change features |
| `-entity-labels` | `SML_ENTITY_LABELS` | `service_name` | Comma-separated entity key labels |
| `-force-drop-labels` | `SML_FORCE_DROP_LABELS` | | Comma-separated labels to drop |
| `-output` | | `json` | Output format: `json`, `csv`, or `dem` |
| `-dem-endpoint` | `SML_SERVER` | | DEM server URL (required for `dem` output) |
| `-dem-project` | `SML_PROJECT_NAME` | | DEM project name (required for `dem` output) |

For DEM output, `SML_KEY_ID` and `SML_SECRET_KEY` must also be set as environment variables.

One of `-prometheus-url` or `-input-file` is required.

## Input: Raw CSV Format

The CSV file must have columns: `timestamp`, `metric`, `labels`, `value`.

```csv
timestamp,metric,labels,value
2026-01-22 00:00:00,http_server_duration,"{'service_name': 'checkout', 'method': 'GET'}",0.45
```

The `labels` column is a stringified Python dict. Both `2006-01-02 15:04:05` and `2006-01-02T15:04:05` timestamp formats are supported.

## Pipeline Flow

```
1. Load data         Prometheus API or CSV file
2. Discover metrics  get_metrics_dataframe2 (Prometheus) or unique metrics from CSV
3. Filter metrics    filter_salient_metrics (latency, throughput, errors, resources)
4. Filter service    filter_by_service(raw_df, [service], service_label)
5. Window            iter_metrics_windows(raw_df, metrics, window_minutes, step)
6. Denormalize       denormalize_metrics(window_df, config) per window
7. Output            JSON lines, CSV, or DEM API stream
```

Progress is printed to stderr. Data output goes to stdout, so you can pipe it:

```bash
otelsmlcli run -input-file data.csv -service checkout -output csv > features.csv
otelsmlcli run -input-file data.csv -service checkout | jq .entity_key
```
