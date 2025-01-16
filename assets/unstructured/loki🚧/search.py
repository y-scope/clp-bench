import os
import subprocess
import time
from datetime import timedelta

from dateutil import parser

# When launched the loki and promtail containers, the ingestion is automatically started
# You could query the current ingested bytes by:
# curl -G http://localhost:3100/metrics | grep 'loki_distributor_bytes_received_total'
# You could query the current compression size by:
# curl -G http://localhost:3100/metrics | grep 'loki_chunk_store_stored_chunk_bytes_total'
# You could query the ingestion time by:
# curl -G http://localhost:3100/metrics | \
# grep 'loki_request_duration_seconds_sum{method="POST",route="loki_api_v1_push"'

script_dir = os.path.dirname(os.path.realpath(__file__))
logcli_binary_path = os.path.join(script_dir, "logcli")
job = "benchlogs"
limit = 2000000
batch = 1000
# The from and to is the time range of compression. For example, compression started after
# 2024-11-03T14:00:00Z and ended before 2024-11-05T16:00:00Z, then from_ts and to_ts will
# be as following
from_ts = "2024-11-03T14:00:00Z"
to_ts = "2024-11-05T16:00:00Z"
interval = timedelta(minutes=30)
queries = [
    '" org.apache.hadoop.hdfs.server.common.Storage: Analyzing storage directories for bpid "',
    '" org.apache.hadoop.hdfs.server.datanode.DataNode: DataTransfer, at "',
    '" INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl: '
    'Container "',
    '" DEBUG org.apache.hadoop.mapred.ShuffleHandler: verifying request. enc_str="',
    '" to pid 21177 as user "',
    '" 10000 reply: "',
    '" 10 reply: "',
    '" 178.2 MB "',
    '" 1.9 GB "',
    '"job_1528179349176_24837"',
    '"blk_1075089282_1348458"',
    '"hdfs://master:8200/HiBench/Bayes/temp/worddict"',
    '" abcde "',
]

start_time = parser.isoparse(from_ts)
end_time = parser.isoparse(to_ts)
for query in queries:
    current_time = start_time
    total_query_latency = 0
    total_nr_matched_log_lines = 0
    while current_time <= end_time - interval:
        command = (
            f"{logcli_binary_path} query "
            + "'{ job="
            + f'"{job}"'
            + "} |~ "
            + query
            + f"' --limit={limit} --batch={batch} "
            + f'--from="{current_time.isoformat()}" '
            + f'--to="{(current_time + interval).isoformat()}" | wc -l'
        )
        start_ts = time.perf_counter_ns()
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            shell=True,
            check=True,
        )
        end_ts = time.perf_counter_ns()
        total_query_latency += (end_ts - start_ts) / 1e9
        total_nr_matched_log_lines += int(result.stdout.decode("utf-8").strip())
        current_time += interval
    print(
        f"Query: {query}, matched log lines: {total_nr_matched_log_lines}, e2e latency: "
        f"{total_query_latency}"
    )
