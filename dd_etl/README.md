# dd_etl

Datadog adapter for the [otel_etl](../otel_etl) pipeline. Intercepts Datadog Agent metric payloads, buffers them in memory, and normalizes them into OTel-style DataFrames so the existing otel_etl transformation pipeline can produce ML-ready wide-format features.

## How It Works

```
DD Agent ──additional_endpoints──▶ FastAPI Receiver ──▶ MetricStore (in-memory)
                                                              │
                                                     fetch_metrics_range()
                                                              │
                                                   otel_etl.denormalize_metrics()
                                                              │
                                                       Wide DataFrame
```

The Datadog Agent's [`additional_endpoints`](https://docs.datadoghq.com/agent/configuration/dual-shipping/) setting sends a copy of all metric payloads to the dd_etl receiver. The receiver parses v1 JSON, v2 JSON, and v2 protobuf formats, normalizes Datadog conventions to OTel conventions, and buffers the data. When you're ready, query the buffer and pipe it through otel_etl's cardinality reduction, type-aware aggregation, and feature generation to get a stable wide-format DataFrame suitable for anomaly detection or ML.

## Installation

```bash
pip install -r requirements.txt
```

Requires the sibling `otel_etl` package to be importable.

## Quickstart

### 1. Start the receiver

```python
from dd_etl import start_receiver

start_receiver(host="0.0.0.0", port=8126)
```

This launches a FastAPI server that mimics the Datadog intake API.

### 2. Configure the Datadog Agent

Add to your `datadog.yaml`:

```yaml
additional_endpoints:
  "http://<receiver-host>:8126":
    - <any-string>
```

The agent will now dual-ship metrics to both Datadog and the dd_etl receiver.

### 3. Generate a schema

**Option A — From the receiver buffer (no API keys needed):**

```python
from dd_etl import run_profiler_from_receiver

schema = run_profiler_from_receiver(
    receiver_url="http://localhost:8126",
    output_path="dd_schema_config.yaml",
)
```

**Option B — From the Datadog API:**

```bash
export DD_API_KEY=<your-api-key>
export DD_APP_KEY=<your-app-key>
```

```python
from dd_etl import run_profiler

schema = run_profiler(output_path="dd_schema_config.yaml")
```

### 4. Transform metrics into features

```python
from dd_etl import start_receiver, denormalize_metrics
from dd_etl.receiver.metric_store import MetricStore

store = MetricStore()

# Fetch buffered data as a standard DataFrame
raw_df = store.fetch_metrics_range(step="60s")

# Transform to wide-format ML-ready features
features_df = denormalize_metrics(raw_df, schema_config="dd_schema_config.yaml")
```

## Receiver API Endpoints

### Intake (DD Agent posts here)

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/series` | POST | DD v1 JSON series payload |
| `/api/v2/series` | POST | DD v2 JSON or protobuf series payload |
| `/intake/` | POST | Auto-detect v1/v2 format |
| `/api/beta/sketches` | POST | Distribution sketches (accepted, discarded) |
| `/api/v1/validate` | GET/POST | Agent validation (always returns valid) |
| `/api/v1/check_run` | GET/POST | Agent check run (always OK) |
| `/api/v1/metadata` | GET/POST | Agent metadata (always OK) |

### Utility

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Buffer stats and last checkpoint |
| `/metrics` | GET | List known metric names and type registry |
| `/query` | GET | Query buffered data with time range and step |
| `/profile` | GET | Analyze buffer for schema generation |

## Normalization

dd_etl translates Datadog conventions to OTel conventions at intake time:

### Tag Mapping

| Datadog Tag | OTel Label |
|---|---|
| `service` | `service_name` |
| `host` | `instance` |
| `env` | `environment` |
| `version` | `service_version` |
| `source` | `telemetry_source` |

### Metric Type Mapping

| DD Type | OTel Type | Name Transform |
|---|---|---|
| `gauge` | gauge | as-is |
| `count` | counter | appends `_total` |
| `rate` | gauge | as-is |
| `distribution` | gauge | as-is |

### Metric Name Convention

Dots are converted to underscores: `system.cpu.user` becomes `system_cpu_user`.

DD histogram sub-metrics (`.avg`, `.count`, `.median`, `.max`, `.95percentile`) are grouped under their base metric family.

## Re-aggregation

The DD agent flushes every 10 seconds. `MetricStore.fetch_metrics_range()` re-aggregates to any requested step size using type-aware logic:

| DD Type | Aggregation |
|---|---|
| `gauge` | Last value in window |
| `count` | Sum of deltas, then cumulative sum across windows |
| `rate` | Mean of rates in window |

Count metrics are converted from deltas to cumulative sums because otel_etl's counter aggregator expects cumulative counters.

## Project Structure

```
dd_etl/
├── __init__.py              # Public API: run_profiler, denormalize_metrics, start_receiver
├── main.py                  # Orchestration: profiler + denormalize delegates to otel_etl
├── config/
│   └── defaults.py          # Tag mappings, type mappings, histogram suffixes, receiver defaults
├── receiver/
│   ├── app.py               # FastAPI intake server
│   ├── payload_parser.py    # v1 JSON, v2 JSON, v2 protobuf parsers
│   ├── metric_store.py      # Thread-safe in-memory ring buffer with re-aggregation
│   └── proto/
│       └── metrics_pb2.py   # Generated protobuf bindings (do not edit)
├── profiler/
│   ├── metric_discovery.py  # Discover metrics from DD API, group into families
│   ├── label_discovery.py   # Discover tags per metric family
│   └── cardinality_analyzer.py  # Analyze tag cardinality, assign tiers
├── utils/
│   ├── tag_mapper.py        # DD→OTel normalization (tags, names, types)
│   ├── datadog_api_client.py # Wrapper around datadog-api-client SDK
│   └── checkpoint.py        # High-water-mark persistence for gap detection
└── requirements.txt
```

## Dependencies

- **fastapi** / **uvicorn** — receiver server
- **datadog-api-client** — profiler API access (only needed for API-mode profiling)
- **pandas** / **numpy** — data manipulation
- **protobuf** — v2 payload parsing
- **pyyaml** / **requests** — schema I/O
- **otel_etl** — the core ETL pipeline (sibling package)
