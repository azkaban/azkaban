#!/bin/bash
script_dir=$(dirname $0)

source "${script_dir}/internal/startup-shared.sh"

# pass along command line arguments to the internal launch script.
log_file="executorServerLog__$(date +%F+%T).out"
startup_cmd="${script_dir}/internal/internal-start-executor.sh $@"

launch "${startup_cmd}" "${log_file}"
