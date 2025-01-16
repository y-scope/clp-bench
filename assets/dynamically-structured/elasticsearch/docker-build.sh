#!/usr/bin/env bash

set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
container_name=$(cat "$script_dir/container-name")

docker build \
    --tag "$container_name" \
    "$script_dir"
