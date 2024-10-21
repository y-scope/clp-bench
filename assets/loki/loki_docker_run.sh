#!/bin/bash
# Just make sure the loki-config.yaml under the same directory
docker run --name loki -d -v $(pwd):/mnt/config -p 3100:3100 grafana/loki:3.0.0 -config.file=/mnt/config/loki-config.yaml
