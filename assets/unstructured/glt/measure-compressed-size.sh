#!/usr/bin/env bash

data_path=/home/archives
find "$data_path" -exec chmod o+r+x {} \;
du "$data_path" -bc | awk "END {print \$1}"
