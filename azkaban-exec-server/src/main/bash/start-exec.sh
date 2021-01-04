#!/bin/bash

script_dir=$(dirname $0)

# pass along command line arguments to the internal launch script.
${script_dir}/internal/internal-start-executor.sh "$@" >executorServerLog__`date +%F+%T`.out 2>&1 &

