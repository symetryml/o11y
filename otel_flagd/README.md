# otel_flagd

CLI/TUI tool to control [OpenTelemetry Demo](https://opentelemetry.io/docs/demo/) feature flags (via flagd JSON config files) and Locust load generation.

## Quick Start

Requires Python 3.12+. Tested with [uv](https://docs.astral.sh/uv/).

```bash
uv venv p312 --python 3.12 --seed
source p312/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
# Run directly (no install needed)
python otel_flagd

# Run with subcommands
python otel_flagd flag list
python otel_flagd load status
python otel_flagd scenario list
python otel_flagd tui
```

Or install as a package for the `otelfl` command:

```bash
pip install -e .
otelfl --help
```

## Fetch Prometheus Metrics

Fetch all Prometheus metrics for a time window and save to CSV. Uses the `otel_etl` library.

`PYTHONPATH=./ otelfl fetch --url http://localhost:9090 --outfile ./somename.csv --minutes 5`

```
PYTHONPATH=./ otelfl fetch --url http://localhost:9090 --jaeger-url http://localhost:16685 --opensearch-url http://localhost:9200 --outfile ./somename.csv --minutes 5

```

```bash
# Fetch last 60 minutes of metrics
otelfl fetch --url http://localhost:9090 --outfile metrics.csv --minutes 60

# Custom step resolution
otelfl fetch --url http://localhost:9090 --outfile metrics.csv --minutes 30 --step 30s
```

| Flag | Required | Default | Purpose |
|------|----------|---------|---------|
| `--url` | yes | — | Prometheus base URL |
| `--outfile` | yes | — | Output CSV file path |
| `--minutes` | no | 60 | Minutes of history to fetch |
| `--step` | no | 60s | Query resolution step |

## Configuration

Settings can be overridden with environment variables:

| Env Var | Default | Purpose |
|---------|---------|---------|
| `OTELFL_FLAGD_CONFIG` | `~/sideProjects/opentelemetry-demo/src/flagd/demo.flagd.json` | Path to flagd config JSON |
| `OTELFL_LOCUST_URL` | `http://localhost:8080/loadgen/` | Locust API base URL |
| `OTELFL_POLL_INTERVAL` | `2.0` | Poll interval in seconds |

## Architecture

```
CLI (otelfl/cli/)  ──┐
                     ├──▶  Core Services (otelfl/core/)
TUI (otelfl/tui/)  ──┘
```

- **Core services** — flagd file client, Locust HTTP client, experiment logger, run modes, chaos scenarios
- **CLI** — argparse subcommands: `flag`, `load`, `stats`, `experiment`, `scenario`, `fetch`, `tui`
- **TUI** — Textual app with 2x2 grid: Flag Panel, Load Panel, Stats Panel, Timeline Panel

```bash
export OTELFL_JAEGER_URL=http://localhost:16685
export OTELFL_OPENSEARCH_URL=http://localhost:9200
export OTELFL_METRICS_URL=http://localhost:9090
export OTELFL_LOCUST_URL=http://localhost:8080/loadgen
export OTELFL_FLAGD_URL=http://localhost:8080/feature
export OTELFL_PROMETHEUS_URL=http://localhost:9090

export PYTHONPATH=$(pwd)/..

sh scenarios/all_failures_5mins.sh

otelfl fetch --url http://localhost:9090 --jaeger-url $OTELFL_JAEGER_URL --opensearch-url $OTELFL_OPENSEARCH_URL --outfile ./fetch_results/baseline10m.csv --minutes 10
```

```bash
export OTELFL_JAEGER_URL=http://localhost:16685
export OTELFL_OPENSEARCH_URL=http://localhost:9200

export OTELFL_METRICS_URL=http://localhost:9090
export OTELFL_LOCUST_URL=http://localhost:8080/loadgen
export OTELFL_FLAGD_URL=http://localhost:8080/feature
export OTELFL_PROMETHEUS_URL=http://localhost:9090

export PYTHONPATH=$(pwd)/..

sh ./scenarios/all_failures_Xmins.sh


otelfl fetch --url http://localhost:9090 --jaeger-url $OTELFL_JAEGER_URL --opensearch-url $OTELFL_OPENSEARCH_URL --outfile ./fetch_results/baseline60m.csv --minutes 60 --chunk-minutes 2 

```



