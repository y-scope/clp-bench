#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Datasets path argument is missing."
    echo "Usage: bash ./ingest.sh <absolute_datasets_path_in_container>"
    exit 1
fi

glt_binary=/home/assets/glt
data_path=/home/archives

"${glt_binary}" c "$data_path" "$1"
