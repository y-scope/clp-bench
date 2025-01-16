#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Query argument is missing."
    echo "Usage: bash ./search.sh <query>"
    exit 1
fi

clp_s_binary=/home/assets/clp-s
data_path=/home/archives

"${clp_s_binary}" s "$data_path" "$1" | wc -l
