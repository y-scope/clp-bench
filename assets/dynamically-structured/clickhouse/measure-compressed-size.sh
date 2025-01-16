#!/usr/bin/env bash

collection_name=clickhouse_clp_bench

clickhouse-client \
    --query "SELECT SUM(bytes) FROM system.parts WHERE active AND table = '${collection_name}'"
