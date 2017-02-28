#!/bin/bash

base_dir=$(dirname $0)/..

$base_dir/bin/azkaban-web-start.sh $base_dir >$base_dir/logs/webServerLog_`date +%F+%T`.out 2>&1 &
