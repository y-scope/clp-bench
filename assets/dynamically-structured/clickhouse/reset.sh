#!/usr/bin/env bash

collection_name=clickhouse_clp_bench

clickhouse-client \
    --max_threads 1 \
    --query "DROP TABLE IF EXISTS ${collection_name}" >/dev/null 2>&1
clickhouse-client \
    --max_threads 1 \
    --query "CREATE TABLE ${collection_name}(raw String CODEC(ZSTD(3))) ENGINE = MergeTree ORDER \
    BY tuple()" >/dev/null 2>&1
