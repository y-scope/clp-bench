#!/usr/bin/env bash

collection_name=mongodb_clp_bench
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

bash "${script_dir}/terminate.sh"
bash "${script_dir}/launch.sh"
mongosh logs --eval "'db.${collection_name}.storageSize().toString()'"
