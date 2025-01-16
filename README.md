# clp-bench

**clp-bench** is a tool for benchmarking [glt] and other log management systems. It functions as a 
Python package and includes a [web interface][ui] for displaying benchmark results.

## Requirements

- Docker
- Python v3.10 or higher

# Setup

```shell
python3 -m venv venv
. venv/bin/activate
pip install -e .
```

To view usage instructions, run `clp-bench --help`.

# Results
You can view the current benchmark results [here][webui]. The benchmark currently evaluates
ingestion and query performance for the following tools:

| Tool                           | Version     |
|--------------------------------|-------------|
| [ClickHouse][clickhouse]       | 23.3.1.2823 |
| [glt][glt]                     | 0.2.0       |
| [clp-s][clp-s]                 | 0.2.0       |
| [Elasticsearch][elasticsearch] | 8.6.2       |
| `grep`                         | 3.7         |
| [Loki][loki]                   | 3.0.0       |
| [MongoDB][mongodb]             | 6.0.19      |
| [Splunk][splunk]               | 9.3.2       |

Don't see a tool here? Feel free to file a [GitHub issue][new-issue] for one or follow this
[guide][adding-a-tool] for how to add one. 

For a detailed description of the benchmarking 
methodology, see [here][methodology].

[assets]: assets
[clickhouse]: https://clickhouse.com/
[glt]: https://github.com/y-scope/clp
[clp-s]: https://docs.yscope.com/clp/main/user-guide/core-clp-s.html
[webui]: https://benchmarks.yscope.com/clp/
[elasticsearch]: https://www.elastic.co/downloads/elasticsearch
[loki]: https://grafana.com/oss/loki/
[mongodb]: https://www.mongodb.com/
[methodology]: docs/methodology.md
[new-issue]: https://github.com/y-scope/clp-bench/issues/new
[adding-a-tool]: docs/adding-a-tool.md
[splunk]: https://www.splunk.com/
[ui]: ui
