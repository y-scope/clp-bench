#!/bin/bash
chown -R elasticsearch:elasticsearch /var/lib/elasticsearch

sed -i 's/#http.port: 9200/http.port: 9201/' /etc/elasticsearch/elasticsearch.yml
sed -i 's/xpack.security.enabled: true/xpack.security.enabled: false/' /etc/elasticsearch/elasticsearch.yml
sed -i '/cluster.initial_master_nodes/d' /etc/elasticsearch/elasticsearch.yml
grep -q 'discovery.type: single-node' /etc/elasticsearch/elasticsearch.yml || echo 'discovery.type: single-node' >> /etc/elasticsearch/elasticsearch.yml

sed -i '/elasticsearch/ s/\/bin\/false/\/bin\/bash/' /etc/passwd
sed -i '/elasticsearch/ s/\/nonexistent/\/usr\/share\/elasticsearch/' /etc/passwd

su elasticsearch -c '/usr/share/elasticsearch/bin/elasticsearch -d'