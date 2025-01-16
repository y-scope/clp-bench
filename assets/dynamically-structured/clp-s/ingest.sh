#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Datasets path argument is missing."
    echo "Usage: bash ./ingest.sh <absolute_datasets_path_in_container>"
    exit 1
fi

clp_s_binary=/home/assets/clp-s
data_path=/home/archives

"${clp_s_binary}" c --timestamp-key "t.\$date" --target-encoded-size 268435456 "$data_path" "$1"
