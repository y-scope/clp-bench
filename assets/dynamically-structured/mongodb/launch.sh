#!/usr/bin/env bash

# Check if MongoDB is installed
if ! command -v mongod &>/dev/null; then
    exit 1
fi

# Create the data directory if it doesn't exist
mkdir -p /data/db

# Start MongoDB with specified options
mongod \
    --fork \
    --syslog \
    --wiredTigerCollectionBlockCompressor zstd \
    --zstdDefaultCompressionLevel 3 \
    >/dev/null 2>&1

# Wait for 2 seconds to ensure MongoDB starts
sleep 2
