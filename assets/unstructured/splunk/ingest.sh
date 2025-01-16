#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Datasets path argument is missing."
    echo "Usage: bash ./ingest.sh <absolute_datasets_path_in_container>"
    exit 1
fi

nr_log_files=$(find datasets/ -type f -name "*log*" | wc -l)
/opt/splunk/bin/splunk add monitor "$1" -auth "admin:admin_password"

while true; do
    count=$(
        grep "Batch input finished reading file=" /opt/splunk/var/log/splunk/splunkd.log \
        | grep "$1" \
        | sed -n "s/.*file='\([^']*\)'.*/\1/p" \
        | wc -l
    )
    
    if [ "$count" -eq "$nr_log_files" ]; then
        exit 0
    fi
    
    sleep 1
done
