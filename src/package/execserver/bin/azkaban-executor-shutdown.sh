#!/bin/bash
azkaban_dir=$(dirname $0)/..

executorport=`cat $azkaban_dir/conf/azkaban.properties | grep executor.port | cut -d = -f 2`
echo "Shutting down current running AzkabanExecutorServer at port $executorport"

proc=`cat $azkaban_dir/currentpid`

kill $proc

cat /dev/null > $azkaban_dir/currentpid
