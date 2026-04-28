#!/usr/bin/env bash
# Scenario: Normal → Spike → Normal
# Starts in normal mode, spikes to 20 users for 5 minutes, then returns to normal.

set -euo pipefail

TS=${1:-spike_test}
OTELFL_ARGS="--ts $TS"
[ -n "${OTELFL_LOCUST_URL:-}" ] && OTELFL_ARGS="$OTELFL_ARGS --locust-url $OTELFL_LOCUST_URL"
[ -n "${OTELFL_FLAGD_URL:-}" ] && OTELFL_ARGS="$OTELFL_ARGS --flagd-url $OTELFL_FLAGD_URL"

echo "=== Starting normal mode ==="
otelfl $OTELFL_ARGS load start --mode normal

echo "=== Waiting 5 seconds before spike ==="
sleep 5

echo "=== Spiking to 20 users for 2 minutes ==="
otelfl $OTELFL_ARGS load start --mode high --run-time 2m

echo "=== Waiting 2 minutes for spike to complete ==="
sleep 120

echo "=== Returning to normal mode ==="
otelfl $OTELFL_ARGS load start --mode normal

echo "=== Scenario complete ==="
