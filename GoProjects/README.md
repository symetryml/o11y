# GoProjects

Go implementation of the SymetryML observability pipeline. Three modules that work together to ingest, denormalize, and export OpenTelemetry metrics for machine learning.

## Modules

### godf

Lightweight, pandas-inspired DataFrame library for Go. Provides the data manipulation primitives that `oteletl` and `otelsml` are built on.

**Key features:** Series (typed, nullable columns), DataFrame, GroupBy with aggregation, PivotTable, Melt, Concat, Sort, Rolling windows, Shift, arithmetic operations, string filtering.

```go
import "github.com/symetryml/godf"

df := godf.NewDataFrame(records)
wide := df.PivotTable([]string{"timestamp", "entity_key"}, "feature", "value", "first")
grouped := df.GroupBy("entity").Shift("value", 5)
```

### oteletl

Go port of the Python `otel_etl` pipeline. Transforms raw OTel metrics into ML-ready wide-format feature matrices.

**Packages:**
- `pipeline` -- `DenormalizeMetrics()` orchestration, filters (`FilterByService`, `FilterSalientMetrics`), profiler (`RunProfilerFromDataFrame`)
- `transformer` -- Status code, HTTP method, operation, and route bucketing
- `classifier` -- Semantic label classification, metric family/type detection
- `aggregator` -- Histogram percentile estimation, counter rate/delta, gauge summary stats
- `prometheus` -- Prometheus HTTP API client, `FetchMetricsRangeDF`, `IterMetricsWindows`

```go
import "github.com/symetryml/oteletl/pipeline"

cfg := pipeline.DefaultConfig()
wide := pipeline.DenormalizeMetrics(rawDF, cfg)
```

### otelsml

Custom OpenTelemetry Collector that chains after a stock OTel Collector. Receives OTLP metrics, runs the `DenormalizeMetrics` pipeline, and exports wide-format ML features.

**Components:**
- `smlprocessor` -- OTel Collector processor: OTLP in, denormalized features out
- `smlexporter` -- Exports features as JSON lines or wide-format CSV

**Architecture:**
```
Any telemetry source
    |
Stock OTel Collector (port 4317/4318)
    | OTLP export
    v
otelsml (port 4319/4320)
    | otlp receiver -> smlprocessor -> smlexporter
    v
stdout / file / DEM API (future)
```

## Environment variable
```bash
export SML_SCHEMA_PATH=/path/to/schema_config-otel001.yaml
export SML_WINDOW_SECONDS=60
export SML_INCLUDE_DELTAS=
export SML_ENTITY_LABELS=
export SML_FORCE_DROP_LABELS=


export SML_SCHEMA_PATH=/path/to/schema_config.yaml
export SML_SERVICES=checkout
export SML_SERVICE_LABEL=service_name
export SML_SANITIZE_NAMES=true #true: container.cpu.usage.total → container_cpu_usage_total (matches Prometheus-profiled schemas)
# otelsml / demclient
export SML_SERVER=http://your-sml-server:8080
export SML_KEY_ID=your-key-id
export SML_SECRET_KEY=your-secret-key
export SML_PROJECT_NAME=otel_conn
```


## Build

```bash
# Prerequisites
brew install go  # Go 1.26+

# Build all modules
cd godf && go test ./... && cd ..
cd oteletl && go test ./... && cd ..
cd otelsml && go build -o otelsml ./cmd/otelsml/

# Run the collector
cd otelsml && ./otelsml --config config.yaml
```

## Test

```bash
# Unit tests (all modules)
cd godf && go test ./...       # 83 tests
cd oteletl && go test ./...    # 91 tests
cd otelsml && go test ./...    # 9 tests

# Parity tests against Python (requires otel_etl + baseline10m.csv)
cd oteletl
python3 testdata/generate_parity.py
go test ./pipeline/ -run TestParity -v
```

## Configuration

otelsml is configured via YAML (same format as any OTel Collector):

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
    window_seconds: 60
    include_deltas: true
    delta_windows: [5, 60]
    # entity_labels: [service_name]    # override auto-detect
    # force_drop_labels: [trace_id]    # always drop these

exporters:
  # Wide-format CSV (one row per timestamp/entity, features as columns)
  smlexporter:
    format: csv          # or "json"
    output_path: stdout  # or /path/to/file.csv

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [smlprocessor]
      exporters: [smlexporter]
```

## Integration with OpenTelemetry Demo

See the `docker-compose-opentelemetry-demo` for file configuration. This folder replicate
the `opentelemetry-demo` folder hierarchy so that it is easy to know which files goes were.
