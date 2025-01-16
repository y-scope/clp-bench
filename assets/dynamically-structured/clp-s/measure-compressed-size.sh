#!/usr/bin/env bash

data_path=/home/archives
du "$data_path" -bc | awk "END {print \$1}"
