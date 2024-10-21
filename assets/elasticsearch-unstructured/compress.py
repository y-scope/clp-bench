#!/usr/bin/python3
import json
import logging
import os
import pathlib
import requests
import time
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import parallel_bulk, streaming_bulk
import sys
import glob

logging.basicConfig(format='%(asctime)s [%(pathname)s:%(lineno)d] - %(message)s', datefmt='%y-%b-%d %H:%M:%S', level=logging.INFO)
es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)
es_transport_logger = logging.getLogger('elastic_transport.transport')
es_transport_logger.setLevel(logging.WARNING)

original_sizes = []
compressed_sizes = []
compression_ratios = []
ingestion_times = []
ingestion_speeds = []

# path_pattern = '/home/datasets/worker*/worker*/*log*'
path_pattern = sys.argv[1]
# Find all files matching the pattern
log_files = glob.glob(path_pattern)
logging.info(f'Total log files: {len(log_files)}')

def get_compressed_size(dataset):
    response = requests.get(f'http://localhost:9201/{dataset.replace("-", "_")}/_stats').json()
    return response['_all']['total']['store']['size_in_bytes']


def traverse_data(index_name):
    for log_file in log_files:
        logging.info(f'Processed file: {log_file}')
        with open(log_file, 'r') as f:
            for line in f.readlines():
                yield {
                    '_index': index_name,
                    '_source': {
                        'log_line': line
                    }
                }


def ingest_dataset():
    es = Elasticsearch('http://localhost:9201', request_timeout=1200, retry_on_timeout=True)
    dataset='hadoop'
    index_name = dataset

    requests.delete(f"http://localhost:9201/{index_name}")
    logging.info(f'Begin ingesting {dataset}')
    start_time = time.time()
    count = 0
    for success, info in streaming_bulk(es, traverse_data(dataset), raise_on_error=False, raise_on_exception=False, chunk_size=100000, request_timeout=3600):
        if success:
            count += 1
        else:
            logging.error(f"Failed to index document at {count}: {info}")
        if count % 1000000 == 0:
            logging.info(f'Index {count} logs')

    requests.post(f"http://localhost:9201/{index_name}/_flush/")
    logging.debug(f'Flush all data in {index_name}')
    end_time = time.time()
    logging.info(f'Finish ingesting {dataset}')

    file_size = 0
    for log_file in log_files:
        file_size += os.path.getsize(log_file)
    time.sleep(5)
    compressed_size = get_compressed_size(dataset)
    compression_ratio  = file_size / compressed_size
    ingestion_time = end_time - start_time
    ingestion_speed = file_size / ingestion_time / 1024 / 1024


    logging.info(f'Original size for {dataset} is {file_size}')
    logging.info(f'Compressed size for {dataset} is {compressed_size}')
    logging.info(f'Compression ratio for {dataset} is {compression_ratio}')
    logging.info(f'Ingestion time for {dataset} is {ingestion_time} s')
    logging.info(f'Ingestion speed for {dataset} is {ingestion_speed} MB/s')


if __name__ == "__main__":
    try:
        ingest_dataset()
    except Exception as e:
        print(e)
    
