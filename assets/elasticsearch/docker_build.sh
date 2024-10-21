#!/bin/bash

set -e
set -u

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
container_name="elasticsearch-semi-xiaochong"

docker build \
	-t "$container_name" \
	"$script_dir" \
	--file "$script_dir"/Dockerfile
