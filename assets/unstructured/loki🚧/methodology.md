# Basic Information
| Version | Download Link (Image or Binary) |
|---------|---------------------------------|
| 3.0.0   | 💾[Download][download]          |

# Specifics
[loki] runs with two microservices. Loki itself is working as a backend which ingests the data sent
by the log collector. We use [promtail] as its log collector. These two are running in two
containers communicated through REST APIs. For query benchmark, we use [logcli] to execute queries.

We haven't integrated launching and ingesting for Loki into `clp-bench` yet, so you may need to
manually launch and ingest data first, then use `clp-bench` to run the query benchmark.

## Launch

To run Loki, create a `loki-config.yaml` configuration file as the following (see details
[here][loki-config]):

```yaml
auth_enabled: false

server:
  http_listen_port: 3100
  grpc_listen_port: 9096

common:
  instance_addr: 127.0.0.1
  path_prefix: /tmp/loki
  storage:
    filesystem:
      chunks_directory: /tmp/loki/chunks
      rules_directory: /tmp/loki/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory

query_range:
  results_cache:
    cache:
      embedded_cache:
        enabled: true
        max_size_mb: 1000

schema_config:
  configs:
    - from: 1972-10-24
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h

limits_config:
  reject_old_samples: false
  ingestion_rate_mb: 1000

ruler:
  alertmanager_url: http://localhost:9093
```

Then launch a container for Loki with the following command (assuming you are currently at the
directory which contains the `yaml` configuration file above):

```shell
docker run \
  --name loki \
  -d \
  -v $(pwd):/mnt/config \
  -p 3100:3100 \
  grafana/loki:3.0.0 \
  -config.file=/mnt/config/loki-config.yaml
```

To run Promtail, create a `promtail-config.yaml` file as the following:

```yaml
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: system
    static_configs:
      - targets:
          - localhost
        labels:
          job: benchlogs
          __path__: /mnt/datasets/hadoop/worker*/*
```

Note that `__path__` should be the pattern of directories which contain the log files.

Then launch a container for Promtail with the following command (assuming you are currently at the
directory which contains the above mentioned `yaml` configuration file):

```shell
docker run \
  --name promtail \
  -d \
  -v $(pwd):/mnt/config \
  -v /path/to/hadoop-log-datasets:/mnt/datasets/hadoop \
  --link loki \
  grafana/promtail:3.0.0 \
  -config.file=/mnt/config/promtail-config.yaml
```

When containers for Loki and Promtail have been launched, run the following command until it prints
`ready` , then Loki will start ingesting data:

```shell
curl -G http://localhost:3100/ready
```

## Ingest

Loki ingests data automatically when connects to Promtail. We do not do any preprocessing for the
dataset.

We use `docker stats` to get the memory usage of ingesting data periodically (the frequency should
be consistent with the `yaml` configuration file for `clp-bench as mentioned in the next section)
until the ingestion finishes:

```shell
docker stats loki promtail --no-stream
```

Since Loki does not have prompt when ingestion finishes, we monitor the current ingested data by the
following command:

```shell
curl -G http://localhost:3100/metrics | grep 'loki_distributor_bytes_received_total'
```

When the above command prints the size that equals to the size of the dataset, it means the
ingestion finishes. Then we run the following command to get the time spent for ingesting data:

```shell
curl -G http://localhost:3100/metrics | \
  grep 'loki_request_duration_seconds_sum{method="POST",route="loki_api_v1_push"'
```

## Query Benchmarking

The configuration of query benchmarking is in `search.py` .

Note that in the configuration:

- `job` should match the `job` of the `labels` in the `promtail-config.yaml` (see the example of
  `promtail-config.yaml` ).
- `limit` should be at least the maximum number of matched log lines of the query.
- `batch` is maximum number of matched log lines that Loki will send to the client at once.
- `from_ts` and `to_ts` define the rough wall-clock time range of ingesting data. In this example,
  it means that data ingestion happened between `2024-10-08T10:00:00Z` and `2024-10-09T10:00:00Z` .
- `interval` defines the time range, in minutes, that Loki will use to query log lines ingested
  within that period. For example, setting an interval of 30 means the time range between from and
  to will be divided into 30-minute slices. Loki will run the query on log lines ingested during
  each of these slices. For each query, `clp-bench` instructs Loki to execute the query across all
  time slices, ensuring the entire dataset is covered.

For each query, `clp-bench` run the following command. Note that the `time_slice_start` and
`time_slice_to` are the start and end timestamps for each time slice, `clp-bench` will iterate all
time slices between `from` and `to` in the configuration to cover the entire dataset.

```shell
<logcli_binary_path> \
  query '{ job="{job}" } |~ {query}' \
  --limit={limit} \
  --batch={batch} \
  --from="{time_slice_start}" \
  --to="{time_slice_to}"
```

For measuring memory usage during query execution, we employ the same method used for data
ingestion.

[download]: https://github.com/grafana/loki/releases/tag/v3.0.0
[logcli]: https://grafana.com/docs/loki/latest/query/logcli/
[loki-config]: https://grafana.com/docs/loki/latest/configure/
[loki]: https://grafana.com/oss/loki/
[promtail]: https://grafana.com/docs/loki/latest/send-data/promtail/
