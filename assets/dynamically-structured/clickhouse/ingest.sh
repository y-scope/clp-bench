#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Datasets path argument is missing."
    echo "Usage: bash ./ingest.sh <absolute_datasets_path_in_container>"
    exit 1
fi

collection_name=clickhouse_clp_bench

clickhouse-client \
    --max_threads 1 \
    --query "INSERT INTO ${collection_name} FROM INFILE '$1' FORMAT JSONAsString" \
    >/dev/null 2>&1
