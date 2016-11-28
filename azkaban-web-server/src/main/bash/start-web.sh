#!/bin/bash

base_dir=$(dirname $0)/..

bin/azkaban-web-start.sh $base_dir >logs/webServerLog_`date +%F+%T`.out 2>&1 &
