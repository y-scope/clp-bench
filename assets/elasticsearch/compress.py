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

log_path = sys.argv[1]
# log_path = '/home/muslope/mongodb-test/mongod.log.2023-03-22T03-45-46'
# log_path = '/home/muslope/datasets/mongod.log'

def get_compressed_size(dataset):
    response = requests.get(f'http://localhost:9202/{dataset.replace("-", "_")}/_stats').json()
    return response['_all']['total']['store']['size_in_bytes']


def traverse_data(index_name):
    with open(log_path, encoding='utf-8') as f:
        for line in f:
            json_line = json.loads(line)
            if 'attr' in json_line:
                attr = json_line['attr']
                if 'uuid' in attr and isinstance(attr['uuid'], dict):
                    uuid = attr['uuid']['uuid']['$uuid']
                    json_line['attr']['uuid'] = uuid
                if 'error' in attr and isinstance(attr['error'], str):
                    error_msg = attr['error']
                    json_line['attr']['error'] = {}
                    json_line['attr']['error']['errmsg'] = error_msg
                if 'command' in attr:
                    command = attr['command']
                    if isinstance(command, str):
                        json_line['attr']['command'] = {}
                        json_line['attr']['command']['command'] = command
                    if isinstance(command, dict) and \
                        'q' in command and isinstance(command['q'], dict) and \
                        '_id' in command['q'] and not isinstance(command['q']['_id'], dict):
                        id_value = str(command['q']['_id'])
                        json_line['attr']['command']['q']['_id'] = {}
                        json_line['attr']['command']['q']['_id']['_ooid'] = id_value
                if 'writeConcern' in attr and isinstance(attr['writeConcern'], dict) and \
                    'w' in attr['writeConcern'] and isinstance(attr['writeConcern']['w'], int):
                    w = attr['writeConcern']['w']
                    json_line['attr']['writeConcern']['w'] = str(w)
                if 'query' in attr and isinstance(attr['query'], dict) and \
                    '_id' in attr['query'] and not isinstance(attr['query']['_id'], dict):
                    id_value = str(attr['query']['_id'])
                    json_line['attr']['query']['_id'] = {}
                    json_line['attr']['query']['_id']['_ooid'] = id_value
            yield {
                '_index': index_name,
                '_source': json_line,
            }


def ingest_dataset():
    es = Elasticsearch('http://localhost:9202', request_timeout=1200, retry_on_timeout=True)
    dataset='mongodb_new_single_1'
    index_name = 'mongodb_new_single_1'

    requests.delete(f"http://localhost:9202/{index_name}")
    logging.info(f'Begin ingesting {dataset}')
    start_time = time.time()
    count = 0
    for success, info in streaming_bulk(es, traverse_data(dataset), raise_on_error=False, raise_on_exception=False, chunk_size=10000, request_timeout=120):
        if success:
            count += 1
        else:
            logging.error(f"Failed to index document at {count}: {info}")
        if count % 100000 == 0:
            logging.info(f'Index {count} logs')

    requests.post(f"http://localhost:9202/{index_name}/_flush/")
    logging.debug(f'Flush all data in {index_name}')
    end_time = time.time()
    logging.info(f'Finish ingesting {dataset}')

    file_size = os.path.getsize(log_path)
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
    