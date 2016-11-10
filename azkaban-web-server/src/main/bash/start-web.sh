#!/bin/bash

base_dir=$(dirname $0)/..

bin/azkaban-web-start.sh $base_dir 2>&1>logs/webServerLog_`date +%F+%T`.out &
