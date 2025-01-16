#!/usr/bin/env bash

if test -e /opt/splunk/ftr; then
    echo "The ftr file exists. Splunk may not have been set up yet."
    echo "[user_info]" >/opt/splunk/etc/system/local/user-seed.conf
    echo "USERNAME = admin" >>/opt/splunk/etc/system/local/user-seed.conf
    echo "PASSWORD = admin_password" >>/opt/splunk/etc/system/local/user-seed.conf

    echo "" >>/opt/splunk/etc/splunk-launch.conf
    echo "OPTIMISTIC_ABOUT_FILE_LOCKING=1" >>/opt/splunk/etc/splunk-launch.conf

    {
        echo ""
        echo "[default]"
        echo "TRUNCATE = 0"
        echo "BREAK_ONLY_BEFORE_DATE = false"
        echo "SHOULD_LINEMERGE = false"
    } >>/opt/splunk/etc/apps/SplunkDeploymentServerConfig/default/props.conf

    {
        echo ""
        echo "[default]"
        echo "crcSalt = <SOURCE>"
        echo "move_policy = sinkhole"
    } >>/opt/splunk/etc/apps/SplunkDeploymentServerConfig/default/inputs.conf

    /opt/splunk/bin/splunk start --no-prompt --answer-yes --accept-license
else
    echo "The ftr file does not exist. Splunk has likely been set up before."
    /opt/splunk/bin/splunk start --no-prompt --answer-yes --accept-license
fi
