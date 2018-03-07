#!/usr/bin/env bash
# Shutdown script for the azkaban solo server
set -o nounset

script_dir=$(dirname $0)
base_dir="${script_dir}/.."
source "${script_dir}/internal/util.sh"
common_shutdown "solo-server" ${base_dir}
