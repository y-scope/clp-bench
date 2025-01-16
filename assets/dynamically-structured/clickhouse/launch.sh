#!/usr/bin/env bash

# Start the ClickHouse server in daemon mode
clickhouse-server --daemon

# Wait until ClickHouse server is running
while [ "$(clickhouse-client --query "SELECT 1" 2>/dev/null)" != "1" ]; do
    sleep 1
done
