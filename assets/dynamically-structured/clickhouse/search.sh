#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Query argument is missing."
    echo "Usage: bash ./search.sh <query>"
    exit 1
fi

collection_name=clickhouse_clp_bench

clickhouse-client \
    --max_threads 1 \
    --query "SELECT * from ${collection_name} where $1 SETTINGS max_threads = 1, \
    min_bytes_to_use_direct_io = 1" 2>/dev/null | wc -l
