#!/usr/bin/env bash

# This script takes queries specified in `config.yaml` as command line argument and executes them

set -e
if [ -z "$1" ]; then
    echo "Error: Query argument is missing."
    echo "Usage: bash ./search.sh <query>"
    exit 1
fi

# Your query command goes here
