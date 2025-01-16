#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Datasets path argument is missing."
    echo "Usage: bash ./ingest.sh <absolute_datasets_path_in_container>"
    exit 1
fi

echo "Nothing to do..."
