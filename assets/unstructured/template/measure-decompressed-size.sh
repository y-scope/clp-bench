#!/usr/bin/env bash

# This script measures the raw dataset size before ingestion. Typically unchanged, it takes
# `datasets_path` from `config.yaml` and uses `du -bc` for size calculation in bytes

set -e
if [ -z "$1" ]; then
    echo "Error: Datasets path argument is missing."
    echo "Usage: bash ./measure-decompressed-size.sh <absolute_datasets_path_in_container>"
    exit 1
fi

du "$1" -bc | awk "END {print \$1}"
