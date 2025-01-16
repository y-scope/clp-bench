#!/usr/bin/env bash

collection_name=mongodb_clp_bench

mongosh logs --eval "db.${collection_name}.drop()" >/dev/null 2>&1
