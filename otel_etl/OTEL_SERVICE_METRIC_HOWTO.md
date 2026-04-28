Solution

There are two approaches:

Approach 1: Filter metric names before fetching (Recommended)

Filter your metrics dataframe to get only the metrics for the specific service, then pass those metric names to fetch_and_denormalize:

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from python_src.s000_list_metrics import get_metrics_dataframe2
from otel_etl import fetch_and_denormalize

prometheus_url = "http://localhost:9090"
df_metrics = get_metrics_dataframe2(prometheus_url)

# Filter by service
the_service = "checkout"
df_checkout = df_metrics[df_metrics['service'] == the_service]

# Get the metric names for this service
the_metrics = df_checkout['metric'].unique().tolist()

print(f"Found {len(the_metrics)} metrics for service '{the_service}'")

# Set time range
end_time = datetime.utcnow()
start_time = end_time - timedelta(hours=1)

# Fetch and denormalize
features_df = fetch_and_denormalize(
    prometheus_url=prometheus_url,
    metric_names=the_metrics,  # Pass filtered metrics
    start=start_time,
    end=end_time,
    step="60s",
    schema_config="path/to/schema_config.yaml",  # Optional
    column_registry="path/to/column_registry.yaml",  # Optional
    layers=[1, 2, 3],
    window_seconds=60,
    include_deltas=True,
)

print(f"Generated {len(features_df)} rows × {len(features_df.columns)} columns")

Approach 2: Fetch first, then filter the raw data

This approach fetches all metrics and filters afterward using the built-in filter_by_service utility:

from otel_etl import fetch_and_denormalize
from otel_etl.utils.filters import filter_by_service
from otel_etl.utils.prometheus_client import PrometheusClient
from datetime import datetime, timedelta

prometheus_url = "http://localhost:9090"
the_service = "checkout"

# Fetch raw metrics (you'll need to get them first)
client = PrometheusClient(prometheus_url)
end_time = datetime.utcnow()
start_time = end_time - timedelta(hours=1)

# Get all metric names (or filter from your df_metrics)
metric_names = df_metrics['metric'].unique().tolist()

# Fetch raw data
raw_df = client.fetch_metrics_range(metric_names, start_time, end_time, "60s")

# Filter by service
from otel_etl.utils.filters import filter_by_service
filtered_df = filter_by_service(raw_df, the_service)

# Then denormalize
from otel_etl import denormalize_metrics
features_df = denormalize_metrics(
    filtered_df,
    schema_config="path/to/schema_config.yaml",
    column_registry="path/to/column_registry.yaml",
    layers=[1, 2, 3],
    window_seconds=60,
    include_deltas=True,
)

Additional Filtering Options

The otel_etl library also provides these filter utilities in otel_etl/utils/filters.py:58:

- filter_by_labels(df, service_name='checkout', status_code='200') - Filter by any label
- filter_by_service(df, 'checkout') - Shortcut for filtering by service
- filter_by_metrics(df, r'^http_') - Filter metrics by regex pattern
- exclude_by_labels(df, service_name='test') - Exclude specific label values

You can combine filters if you want to further refine the metrics:

from otel_etl.utils.filters import filter_by_service, filter_by_metrics

# Filter to checkout service AND only HTTP metrics
filtered = filter_by_service(raw_df, 'checkout')
filtered = filter_by_metrics(filtered, r'^http_')

Approach 1 is recommended because it reduces the amount of data fetched from Prometheus, making it more efficient.


## More Explanation:

Approach 1:

  Generated 921 rows × 3638 columns

  Approach 2:

  Generated 41 rows × 842 columns

  WHY IS THERE A DIFFERENCE?

⏺ Because they do different things:

  Approach 1 - You filter the metric names that checkout uses, then fetch ALL data for those metrics. But other services also emit the same metrics (e.g.,
  http_client_request_duration_seconds is emitted by frontend, checkout, cart, etc.). So you get data from ALL services that use those metric names.

  Approach 2 - You fetch data and then filter by the actual service_name label. So you only get rows where service_name == "checkout".

  If you want only checkout data, Approach 2 is correct.

  Here's the proper way to do it:

  from datetime import datetime, timedelta, timezone
  from otel_etl import denormalize_metrics
  from otel_etl.utils.prometheus_client import PrometheusClient
  from otel_etl.utils.filters import filter_by_service
  from python_src.s000_list_metrics import get_metrics_dataframe2

  prometheus_url = "http://localhost:9090"
  the_service = "checkout"

  # Get metrics for this service
  df_metrics = get_metrics_dataframe2(prometheus_url)
  df_checkout = df_metrics[df_metrics['service'] == the_service]
  the_metrics = df_checkout['metric'].unique().tolist()

  # Fetch raw data
  client = PrometheusClient(prometheus_url)
  end_time = datetime.now(timezone.utc)
  start_time = end_time - timedelta(hours=1)

  raw_df = client.fetch_metrics_range(the_metrics, start_time, end_time, "60s")

  # Filter to only checkout service
  filtered_df = filter_by_service(raw_df, the_service)

  # Denormalize
  features_df = denormalize_metrics(
      filtered_df,
      schema_config=None,
      column_registry=None,
      layers=[1, 2, 3],
      window_seconds=60,
      include_deltas=True,
  )

  print(f"Generated {len(features_df)} rows × {len(features_df.columns)} columns")

  This fetches only the relevant metrics AND filters to only the checkout service's data.