#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Datasets path argument is missing."
    echo "Usage: bash ./ingest.sh <absolute_datasets_path_in_container>"
    exit 1
fi

collection_name=mongodb_clp_bench

mongoimport \
    --numInsertionWorkers=1 \
    --db logs \
    --collection "$collection_name" \
    --file "$1" \
    >/dev/null 2>&1
