#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Query argument is missing."
    echo "Usage: bash ./search.sh <query>"
    exit 1
fi

glt_binary=/home/assets/glt
data_path=/home/archives

"${glt_binary}" s "$data_path" "$1" | wc -l
