#!/bin/bash

script_dir=$(dirname $0)
source "${script_dir}/internal/startup-shared.sh"

log_file="soloServerLog__$(date +%F+%T).out"
startup_cmd="${script_dir}/internal/internal-start-solo-server.sh $@"

launch "${startup_cmd}" "${log_file}"
