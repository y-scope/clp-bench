#!/usr/bin/env bash
# Just make sure the loki-config.yaml under the same directory
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
docker rm -f loki
docker run \
    --name loki \
    -d \
    -v "$script_dir":/mnt/config \
    -p 3100:3100 grafana/loki:3.0.0 \
    -config.file=/mnt/config/loki-config.yaml
