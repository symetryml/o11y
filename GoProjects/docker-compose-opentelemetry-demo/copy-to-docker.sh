
#!/bin/sh
#

cp docker-compose.yml ../../../opentelemetry-demo/


cp ./src/otel-collector/otelcol-config-extras.yml ../../../opentelemetry-demo/src/otel-collector/

cp ./src/otelsml/config.yml ../../../opentelemetry-demo/src/otelsml/

cp ./src/flagd/demo.flagd.json ../../../opentelemetry-demo/src/flagd/

cp ./src/jaeger/config.yml ../../../opentelemetry-demo/src/jaeger/
