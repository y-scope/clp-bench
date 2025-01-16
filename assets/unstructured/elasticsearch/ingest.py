import glob
import logging
import sys

import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk  # type: ignore

collection_name = "elasticsearch_clp_bench"

# path_pattern = '/home/datasets/worker*/worker*/*log*'
path_pattern = sys.argv[1]
# Find all files matching the pattern
log_files = glob.glob(path_pattern)
logging.info(f"Total log files: {len(log_files)}")


def traverse_data(collection_name):
    for log_file in log_files:
        logging.info(f"Processed file: {log_file}")
        with open(log_file, "r") as f:
            for line in f.readlines():
                yield {"_index": collection_name, "_source": {"log_line": line}}


def ingest_dataset():
    es = Elasticsearch("http://localhost:9202", request_timeout=1200, retry_on_timeout=True)

    count = 0
    for success, info in streaming_bulk(
        es,
        traverse_data(collection_name),
        raise_on_error=False,
        raise_on_exception=False,
        chunk_size=10000,
        request_timeout=120,
    ):
        if success:
            count += 1
        else:
            logging.error(f"Failed to index document at {count}: {info}")
        if count % 100000 == 0:
            logging.info(f"Index {count} logs")

    requests.post(f"http://localhost:9202/{collection_name}/_flush/")


if __name__ == "__main__":
    try:
        ingest_dataset()
    except Exception as e:
        print(e)
