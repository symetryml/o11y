# o11y - ML-Ready Observability ETL Pipeline

A self-configuring ETL pipeline that transforms high-cardinality OpenTelemetry and Datadog metrics into stable, ML-ready feature DataFrames. Available as a Python library for prototyping and a Go-based OpenTelemetry Collector for production deployment.

## What It Does

```
Raw OTel/Datadog Metrics ──> Profile ──> Transform ──> ML-Ready Features
     (1000s of labels)                                  (wide-format DataFrame)
```

The pipeline automatically:
- **Discovers** metric structure, types, and cardinality from Prometheus or Datadog
- **Classifies** labels by semantics (resource, signal, dimension, correlation)
- **Reduces cardinality** — buckets status codes, parameterizes routes, applies top-N filtering
- **Aggregates** type-aware (counter rates, histogram percentiles, gauge stats)
- **Outputs** stable wide-format DataFrames: one row per timestamp/entity, features as columns

## Project Structure

```
o11y/
├── otel_etl/          Python: Core ETL pipeline (profile, transform, aggregate, feature gen)
├── dd_etl/            Python: Datadog metrics adapter (receives DD Agent payloads via FastAPI)
├── otel_flagd/        Python: CLI/TUI for controlling OpenTelemetry Demo feature flags + load gen
├── otel_synth/        Python: Synthetic metric data generator with ground-truth anomaly labels
├── signals/           Python: Unified data access layer (Prometheus, Jaeger, OpenSearch, Datadog)
├── docker/            Container build files for the Python services (dd_etl receiver image)
└── GoProjects/
    ├── godf/          Go: Pandas-inspired DataFrame library
    ├── oteletl/       Go: Port of otel_etl pipeline
    ├── otelsml/       Go: Custom OpenTelemetry Collector (chains after stock collector)
    ├── demclient/     Go: SymetryML DEM API client
    └── docker-compose-opentelemetry-demo/   Docker integration example
```

## Quick Start

### Python

```bash
# Requires Python 3.10+
pip install pandas numpy pyyaml requests

# Profile metrics and generate schema
python -c "
from otel_etl import run_profiler
schema = run_profiler(prometheus_url='http://localhost:9090', output_path='schema.yaml')
"

# Transform metrics into ML features
python -c "
from otel_etl import fetch_and_denormalize
from datetime import datetime, timedelta

features_df = fetch_and_denormalize(
    prometheus_url='http://localhost:9090',
    start=datetime.utcnow() - timedelta(hours=1),
    end=datetime.utcnow(),
    step='60s',
    schema_config='schema.yaml',
    layers=[1, 2, 3],
    include_deltas=True,
)
print(features_df.shape)
"
```

### Go (OpenTelemetry Collector)

```bash
cd GoProjects/otelsml
go build -o otelsml ./cmd/otelsml/
./otelsml --config config.yaml
```

The Go collector chains after a stock OTel Collector:

```
Services ──OTLP──> Stock OTel Collector (4317/18) ──OTLP──> otelsml (4319/20) ──> CSV / JSON / DEM API
```

See [GoProjects/README.md](GoProjects/README.md) for build instructions and configuration.

## Components

### otel_etl — Core ETL Pipeline

Transforms Prometheus metrics into ML features via automatic profiling, cardinality reduction, and type-aware aggregation. See [otel_etl/README.md](otel_etl/README.md).

### dd_etl — Datadog Adapter

Receives Datadog Agent payloads (v1 JSON, v2 JSON, v2 protobuf) via a FastAPI endpoint that mimics the Datadog intake API. Normalizes to OTel conventions and delegates to otel_etl. See [dd_etl/README.md](dd_etl/README.md).

### otel_flagd — Demo Control CLI/TUI

CLI and interactive TUI for controlling OpenTelemetry Demo feature flags and Locust load generation. Includes metric fetching to CSV. See [otel_flagd/README.md](otel_flagd/README.md).

### otel_synth — Synthetic Data Generator

Learns statistical profiles from real metrics and generates synthetic data with known anomalies for testing and benchmarking. See [otel_synth/README.md](otel_synth/README.md).

### signals — Telemetry Data Access Layer

Unified Python clients for Prometheus, Jaeger (gRPC), OpenSearch, and Datadog APIs. See [signals/](signals/).

### docker — Container Builds

Dockerfiles and build scripts for packaging the Python services as containers. Currently contains `dd-otel-container/`, which builds the `dd-etl-receiver` image (Python 3.11-slim) bundling `dd_etl` + `otel_etl` and exposing the Datadog intake API on port 8126.

```bash
cd docker/dd-otel-container
./build-image.sh   # copies dd_etl/ and otel_etl/ from the repo root and builds dd-etl-receiver
```

### GoProjects — Go Implementation

Production-grade Go implementation including a pandas-inspired DataFrame library (godf), the ETL pipeline port (oteletl), and a custom OTel Collector (otelsml). See [GoProjects/README.md](GoProjects/README.md).

## Configuration

### Environment Variables

**Python (signals/otel_etl):**

| Variable | Default | Purpose |
|----------|---------|---------|
| `PROMETHEUS_URL` | `http://localhost:9090` | Prometheus server URL |

**Go (otelsml):**

| Variable | Default | Purpose |
|----------|---------|---------|
| `SML_SERVICES` | (all) | Comma-separated service filter |
| `SML_SERVICE_LABEL` | `service_name` | Label used to identify services |
| `SML_SCHEMA_PATH` | (auto-profile) | Path to schema YAML |
| `SML_WINDOW_SECONDS` | `60` | Aggregation window size |
| `SML_INCLUDE_DELTAS` | `true` | Add delta/pct-change features |
| `SML_ENTITY_LABELS` | (auto-detect) | Labels for entity key construction |
| `SML_SANITIZE_NAMES` | `false` | Normalize metric names to Prometheus convention |
| `SML_SERVER` | — | SymetryML DEM server URL |
| `SML_KEY_ID` | — | SymetryML API key ID |
| `SML_SECRET_KEY` | — | SymetryML API secret key |
| `SML_PROJECT_NAME` | — | SymetryML project name |

**otel_flagd:**

| Variable | Default | Purpose |
|----------|---------|---------|
| `OTELFL_FLAGD_CONFIG` | — | Path to flagd config JSON |
| `OTELFL_LOCUST_URL` | `http://localhost:8080/loadgen/` | Locust API URL |
| `OTELFL_PROMETHEUS_URL` | `http://localhost:9090` | Prometheus URL |

## Testing

```bash
# Python
cd otel_flagd && pip install -e ".[dev]" && pytest tests/

# Go
cd GoProjects/godf && go test ./...
cd GoProjects/oteletl && go test ./...
cd GoProjects/otelsml && go test ./...

# Python-Go parity tests
cd GoProjects/oteletl
python3 testdata/generate_parity.py
go test ./pipeline/ -run TestParity -v
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
