#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Datasets path argument is missing."
    echo "Usage: bash ./measure-decompressed-size.sh <absolute_datasets_path_in_container>"
    exit 1
fi

bash -c "du $1 -bc" | awk "END {print \$1}"
