#!/bin/bash

# pass along command line arguments to azkaban-executor-start.sh script
bin/azkaban-executor-start.sh "$@" 2>&1>logs/executorServerLog__`date +%F+%T`.out &

