#!/bin/bash
# Replace the datasets to the actual path, also note that modify the promtail-config.yaml, since the data here is like hadoop-258GB/worker1/wroker1/[logs]
docker run --name promtail -d -v $(pwd):/mnt/config -v /home/xiaochong/clp-bench-prototype/tests/datasets/hadoop-258GB:/mnt/datasets/hadoop --link loki grafana/promtail:3.0.0 -config.file=/mnt/config/promtail-config.yaml
