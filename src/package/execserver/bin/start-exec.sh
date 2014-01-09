#!/bin/bash

base_dir=$(dirname $0)/..

bin/azkaban-executor-start.sh $base_dir 2>&1>logs/executorServerLog__`date +%F+%T`.out &

