# Basic Information
| Version | Download Link (Image or Binary) |
|---------|---------------------------------|
| 9.3.2   | 💾[Download][download]          |

# Specifics
We removed the event length restriction (`TRUNCATE = 0`) and configured each log line as an 
individual event (`BREAK_ONLY_BEFORE_DATE = false` and `SHOULD_LINEMERGE = false`).

Data ingestion is handled using `add monitor ${datasets}`. However, we noticed a discrepancy between
the raw data size (257.25 GB) and the size ingested by Splunk (255.61 GB), resulting in a data loss
of 1.64 GB.

Upon investigation, we found that Splunk overlooked certain files during ingestion:
```
/home/datasets/worker1/worker1/SecurityAuth-root.audit
/home/datasets/worker1/worker1/hadoop-root-datanode-8dc59161430b.out
/home/datasets/worker1/worker1/hadoop-root-datanode-8dc59161430b.out.1
/home/datasets/worker1/worker1/hadoop-root-datanode-8dc59161430b.out.2
/home/datasets/worker1/worker1/hadoop-root-datanode-8dc59161430b.out.3
/home/datasets/worker1/worker1/yarn-root-nodemanager-8dc59161430b.out
/home/datasets/worker1/worker1/yarn-root-nodemanager-8dc59161430b.out.1
/home/datasets/worker1/worker1/yarn-root-nodemanager-8dc59161430b.out.2
/home/datasets/worker1/worker1/yarn-root-nodemanager-8dc59161430b.out.3
/home/datasets/worker2/worker2/SecurityAuth-root.audit
/home/datasets/worker2/worker2/hadoop-root-datanode-fc8782b06de1.out
/home/datasets/worker2/worker2/hadoop-root-datanode-fc8782b06de1.out.1
/home/datasets/worker2/worker2/hadoop-root-datanode-fc8782b06de1.out.2
/home/datasets/worker2/worker2/hadoop-root-datanode-fc8782b06de1.out.3
/home/datasets/worker2/worker2/yarn-root-nodemanager-fc8782b06de1.out
/home/datasets/worker2/worker2/yarn-root-nodemanager-fc8782b06de1.out.1
/home/datasets/worker2/worker2/yarn-root-nodemanager-fc8782b06de1.out.2
/home/datasets/worker2/worker2/yarn-root-nodemanager-fc8782b06de1.out.3
/home/datasets/worker3/worker3/SecurityAuth-root.audit
/home/datasets/worker3/worker3/hadoop-root-datanode-8ed8bf9ebb0e.out
/home/datasets/worker3/worker3/hadoop-root-datanode-8ed8bf9ebb0e.out.1
/home/datasets/worker3/worker3/hadoop-root-datanode-8ed8bf9ebb0e.out.2
/home/datasets/worker3/worker3/hadoop-root-datanode-8ed8bf9ebb0e.out.3
/home/datasets/worker3/worker3/yarn-root-nodemanager-8ed8bf9ebb0e.out
/home/datasets/worker3/worker3/yarn-root-nodemanager-8ed8bf9ebb0e.out.1
/home/datasets/worker3/worker3/yarn-root-nodemanager-8ed8bf9ebb0e.out.2
/home/datasets/worker3/worker3/yarn-root-nodemanager-8ed8bf9ebb0e.out.3
```

The total size of these files is 26.21 KB. The most of remaining of missing data is still 
unaccounted for, but they have no impact on query results of the queries we use during benchmarking.

[download]: https://www.splunk.com/en_us/download/splunk-enterprise.html
