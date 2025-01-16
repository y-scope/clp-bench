#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Query argument is missing."
    echo "Usage: bash ./search.sh <query>"
    exit 1
fi

datasets_path=/home/datasets
grep -r "$1" "$datasets_path" | wc -l
