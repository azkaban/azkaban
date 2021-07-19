#!/bin/bash

script_dir=$(dirname $0)
source "${script_dir}/internal/startup-shared.sh"

log_file="webServerLog__$(date +%F+%T).out"
startup_cmd="${script_dir}/internal/internal-start-web.sh $@"

launch "${startup_cmd}" "${log_file}"
