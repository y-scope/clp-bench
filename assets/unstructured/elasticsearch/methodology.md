# Basic Information
| Version | Download Link (Image or Binary) |
|---------|---------------------------------|
| 8.6.2   | 💾[Download][download]          |

# Specifics
We deploy [elasticsearch] in a single-node configuration with the security feature of
[xpack][disabling-xpack] disabled. We use Elasticsearch's Python package for data ingestion and
search operations.

In contrast to dynamically-structured benchmarking, unstructured datasets for Elasticsearch require no
preprocessing.

[download]: https://www.elastic.co/downloads/past-releases/elasticsearch-8-6-2
[disabling-xpack]: https://www.elastic.co/guide/en/elasticsearch/reference/current/security-settings.html
[elasticsearch]: https://www.elastic.co/downloads/elasticsearch
