#!/bin/sh
#

cp /Users/dev/sideProjects/opentelemetry-demo/docker-compose.yml docker-compose.yml


mkdir -p src/otel-collector
cp /Users/dev/sideProjects/opentelemetry-demo/src/otel-collector/otelcol-config-extras.yml src/otel-collector/

mkdir -p src/otelsml
cp /Users/dev/sideProjects/opentelemetry-demo/src/otelsml/config.yml src/otelsml/

mkdir -p src/flagd
cp /Users/dev/sideProjects/opentelemetry-demo/src/flagd/demo.flagd.json src/flagd/

mkdir -p src/jaeger
cp /Users/dev/sideProjects/opentelemetry-demo/src/jaeger/config.yml src/jaeger/
