from elasticsearch import Elasticsearch

try:
    es = Elasticsearch("http://localhost:9201")
    stats = es.nodes.stats(metric=['jvm', 'process'])
    node_id = list(stats['nodes'].keys())[0]
    node_stats = stats['nodes'][node_id]

    print(node_stats['jvm']['mem']['heap_used_in_bytes'])
except Exception:
    print(-1)