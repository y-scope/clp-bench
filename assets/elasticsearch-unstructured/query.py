from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import parallel_bulk, streaming_bulk
from threading import Thread, Event
import logging
import sys

logging.basicConfig(format='%(asctime)s [%(pathname)s:%(lineno)d] - %(message)s', datefmt='%y-%b-%d %H:%M:%S', level=logging.INFO)
es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)
es_transport_logger = logging.getLogger('elastic_transport.transport')
es_transport_logger.setLevel(logging.WARNING)


es = Elasticsearch("http://localhost:9201", timeout=30, max_retries=10, retry_on_timeout=True)

# '{"query": {"bool": {"must": {"match_phrase": {"log_line": " org.apache.hadoop.hdfs.server.common.Storage: Analyzing storage directories for bpid "}}}}, "size": 10000}'
query = sys.argv[1]

# Function to execute a query without cache
def execute_query_without_cache(query):
    # Initialize the scroll
    page = es.search(
        index="hadoop",
        scroll="8m",  # Keep the search context open for 2 minutes
        body=query,
        request_cache=False
    )
    
    for result in page['hits']['hits']:
        print(result)
    
    # Start scrolling
    sid = page['_scroll_id']
    while True:
        page = es.scroll(scroll_id=sid, scroll='8m')
        if not page['hits']['hits']:
            break
        for result in page['hits']['hits']:
            print(result)

    es.indices.clear_cache(index="hadoop")

# Execute the query
execute_query_without_cache(query)
    
