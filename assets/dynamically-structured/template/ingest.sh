#!/usr/bin/env bash

# This script handles data ingestion, with clp-bench measuring the total latency of this script.
# Avoid adding extra operations

set -e
if [ -z "$1" ]; then
    echo "Error: Datasets path argument is missing."
    echo "Usage: bash ./ingest.sh <absolute_datasets_path_in_container>"
    exit 1
fi

# Your ingest command goes here
