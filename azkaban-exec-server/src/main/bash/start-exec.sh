#!/bin/bash

script_dir=$(dirname $0)
script_parent_dir=$(dirname $script_dir)

# pass along command line arguments to the internal launch script.
${script_dir}/internal/internal-start-executor.sh "$@" > ${script_parent_dir}/local/executorServerLog__`date +%F+%T`.out 2>&1 &

