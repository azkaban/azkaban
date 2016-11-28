#!/bin/bash

# pass along command line arguments to azkaban-executor-start.sh script
bin/azkaban-executor-start.sh "$@" >logs/executorServerLog__`date +%F+%T`.out 2>&1 &

