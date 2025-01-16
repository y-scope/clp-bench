#!/usr/bin/env bash

clickhouse-client --query "SYSTEM DROP UNCOMPRESSED CACHE"
clickhouse-client --query "SYSTEM DROP MARK CACHE"
sync
echo 1 >/proc/sys/vm/drop_caches
