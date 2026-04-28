
rm -Rf dd_etl
rm -Rf otel_etl

cp -R ../dd_etl dd_etl
cp -R ../otel_etl otel_etl

docker image rmi dd-etl-receiver
docker build -f Dockerfile.dd-etl -t dd-etl-receiver .


# docker compose --env-file .env --env-file .env.override up --force-recreate --remove-orphans --detach
# docker logs otel-demo-datadog-dd-etl-receiver-1


