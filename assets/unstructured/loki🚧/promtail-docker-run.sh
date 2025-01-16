#!/usr/bin/env bash
# Replace the datasets to the actual path, also note that modify the promtail-config.yaml, since the data here is like hadoop-258GB/worker1/wroker1/[logs]
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
docker rm -f promtail
docker run \
    --name promtail \
    -d \
    -v "${script_dir}":/mnt/config \
    -v "$1":/mnt/datasets/hadoop \
    --link loki grafana/promtail:3.0.0 \
    -config.file=/mnt/config/promtail-config.yaml
