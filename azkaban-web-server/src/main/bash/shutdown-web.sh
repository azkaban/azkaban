#!/usr/bin/env bash
# Shutdown script for azkaban web server
set -o nounset

script_dir=$(dirname $0)
base_dir="${script_dir}/.."
source "${script_dir}/internal/util.sh"
common_shutdown "web-server" ${base_dir}
