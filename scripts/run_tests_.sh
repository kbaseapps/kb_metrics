#!/bin/bash
script_dir="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
cd $script_dir/..
$script_dir/run_docker.sh run -v $script_dir/workdir:/kb/module/work \
    -e SDK_CALLBACK_URL=$1 \
    -e KBASE_SECURE_CONFIG_PARAM_appdev_mongodb_host=localhost \
    -e KBASE_SECURE_CONFIG_PARAM_mongodb_host=localhost \
    -e KBASE_SECURE_CONFIG_PARAM_mongodb_user=admin \
    -e KBASE_SECURE_CONFIG_PARAM_mongodb_pwd=password \
    test/kb_metrics:latest test