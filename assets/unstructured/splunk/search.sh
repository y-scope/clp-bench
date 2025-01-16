#!/usr/bin/env bash

set -e
if [ -z "$1" ]; then
    echo "Error: Query argument is missing."
    echo "Usage: bash ./search.sh <query>"
    exit 1
fi

/opt/splunk/bin/splunk search "index=main \"$1\" | stats count" \
    -auth "admin:admin_password" \
    -preview false 2>/dev/null \
    | awk "END {print \$1}"
