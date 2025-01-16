import os

from elasticsearch import Elasticsearch

collection_name = "elasticsearch_clp_bench"

es = Elasticsearch("http://localhost:9202", timeout=30, max_retries=10, retry_on_timeout=True)
es.indices.clear_cache(index=collection_name)
os.system("sync; echo 1 > /proc/sys/vm/drop_caches")
