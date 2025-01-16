#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Query argument is missing."
    echo "Usage: bash ./search.sh <query>"
    exit 1
fi

collection_name=mongodb_clp_bench

mongoexport --quiet --db logs --collection "$collection_name" --query "$1" | wc -l
