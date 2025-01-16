#!/usr/bin/env bash

# This script builds the container as per the `Dockerfile` in the same directory. Usually, only the
# `container_name` variable should be adjusted to match the `container_id` in `config.yaml`

set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
container_name=$(cat "$script_dir/container-name")

docker build \
    --tag "$container_name" \
    "$script_dir"
