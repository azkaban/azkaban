#!/bin/bash

script_dir=$(dirname $0)

${script_dir}/internal/internal-start-web.sh >webServerLog_`date +%F+%T`.out 2>&1 &
