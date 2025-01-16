#!/usr/bin/env bash

datasets_path=/home/datasets
du "$datasets_path" -bc | awk "END {print \$1}"
