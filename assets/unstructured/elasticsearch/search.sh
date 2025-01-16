#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Query argument is missing."
    echo "Usage: bash ./search.sh <query>"
    exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
python3 "${script_dir}/search.py" "$1"
