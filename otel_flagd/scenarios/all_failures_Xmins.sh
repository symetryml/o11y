#!/usr/bin/env bash
# Scenario: All Failures — One at a Time, 30 Minutes Each
# Cycles through every flagd failure flag, enabling it for 30 minutes,
# then resetting before moving to the next. Total runtime: ~7 hours.

set -uo pipefail

TS=${1:-all_failures_60mins}
OTELFL_ARGS="--ts $TS"
[ -n "${OTELFL_LOCUST_URL:-}" ] && OTELFL_ARGS="$OTELFL_ARGS --locust-url $OTELFL_LOCUST_URL"
[ -n "${OTELFL_FLAGD_URL:-}" ] && OTELFL_ARGS="$OTELFL_ARGS --flagd-url $OTELFL_FLAGD_URL"


METRICS_URL="${OTELFL_METRICS_URL:-http://localhost:9090}"
JAEGER_URL="${OTELFL_JAEGER_URL:-}"
OPENSEARCH_URL="${OTELFL_OPENSEARCH_URL:-}"
OUTDIR="${OTELFL_FETCH_OUTDIR:-./fetch_results}"
mkdir -p "$OUTDIR"

FETCH_ARGS=""
SOURCE_TAG=""
if [ -n "${DD_URL:-}" ]; then
    METRICS_URL="$DD_URL"
    FETCH_ARGS="--use-dd"
    SOURCE_TAG="_dd"
fi
[ -n "$JAEGER_URL" ] && FETCH_ARGS="$FETCH_ARGS --jaeger-url $JAEGER_URL"
[ -n "$OPENSEARCH_URL" ] && FETCH_ARGS="$FETCH_ARGS --opensearch-url $OPENSEARCH_URL"

HOLD=3600  # minutes in seconds

# Each entry: "flagName variant"
# For boolean flags the variant is "on".
# For multi-variant flags we pick the worst/max failure level.
FLAGS=(
    "paymentFailure        100%"
    "imageSlowLoad         10sec"
    "adHighCpu             on"
    "adManualGc            on"
    "adFailure             on"
    "kafkaQueueProblems    on"
    "cartFailure           on"
    "paymentUnreachable    on"
    "emailMemoryLeak       10000x"
    "productCatalogFailure on"
    "recommendationCacheFailure on"
    "loadGeneratorFloodHomepage on"
    "llmInaccurateResponse on"
    "llmRateLimitError     on"
)

TOTAL=${#FLAGS[@]}

echo "=== All-Failures Scenario ==="
echo "    Flags to cycle: $TOTAL"
echo "    Hold per flag:  $(( HOLD / 60 )) minutes"
echo "    Estimated total: $(( TOTAL * HOLD / 60 )) minutes (~$(( TOTAL * HOLD / 3600 )) hours)"
echo ""

echo "=== Saving flag snapshot ==="
SNAPSHOT=$(mktemp /tmp/otelfl_snapshot_XXXXXX).json
otelfl $OTELFL_ARGS flag snapshot "$SNAPSHOT"

echo "=== Resetting all flags to baseline ==="
otelfl $OTELFL_ARGS flag reset all

COUNT=0
for entry in "${FLAGS[@]}"; do
    FLAG=$(echo "$entry" | awk '{print $1}')
    VARIANT=$(echo "$entry" | awk '{print $2}')
    COUNT=$((COUNT + 1))

    echo ""
    echo "=== [$COUNT/$TOTAL] Enabling $FLAG → $VARIANT ==="
    echo "    $(date '+%Y-%m-%d %H:%M:%S')"
    otelfl $OTELFL_ARGS flag set "$FLAG" "$VARIANT"

    echo "    Holding for $(( HOLD / 60 )) minutes..."
    sleep $HOLD

    echo "    Resetting $FLAG"
    otelfl $OTELFL_ARGS flag reset "$FLAG" \
        || echo "    [WARN] Reset failed, continuing..."

    echo "    Waiting 30s for services to settle..."
    sleep 30

    CSVFILE="$OUTDIR/${FLAG}${SOURCE_TAG}_$(date '+%Y%m%d_%H%M%S').csv"
    echo "    Fetching metrics → $CSVFILE"
    [ -n "$JAEGER_URL" ] && echo "    Fetching traces → ${CSVFILE%.csv}_traces.csv"
    [ -n "$OPENSEARCH_URL" ] && echo "    Fetching logs   → ${CSVFILE%.csv}_logs.csv"
    otelfl $OTELFL_ARGS fetch --url "$METRICS_URL" $FETCH_ARGS --chunk-minutes 2 --outfile "$CSVFILE" --minutes $(( HOLD / 60 + 1 )) \
        || echo "    [WARN] Fetch failed, continuing..."
done

echo ""
echo "=== Restoring original flag snapshot ==="
otelfl $OTELFL_ARGS flag restore "$SNAPSHOT"
rm -f "$SNAPSHOT"

echo "=== Scenario complete at $(date '+%Y-%m-%d %H:%M:%S') ==="
