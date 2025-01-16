#!/usr/bin/env bash

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
bash "${script_dir}/terminate.sh"
/opt/splunk/bin/splunk clean eventdata -f
bash "${script_dir}/launch.sh"
/opt/splunk/bin/splunk clean kvstore -all -f -auth "admin:admin_password"
