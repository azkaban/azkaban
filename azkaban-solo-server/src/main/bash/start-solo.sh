#!/bin/bash

script_dir=$(dirname $0)
script_parent_dir=$(dirname $script_dir)

${script_dir}/internal/internal-start-solo-server.sh "$@" > ${script_parent_dir}/local/soloServerLog__`date +%F+%T`.out 2>&1 &
