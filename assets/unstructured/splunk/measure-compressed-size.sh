#!/usr/bin/env bash

/opt/splunk/bin/splunk search "| dbinspect index=main | stats sum(sizeOnDiskMB)" \
    -auth "admin:admin_password" 2>/dev/null \
    | awk 'END {printf "%.0f\n", $1 * 1024 * 1024}'
