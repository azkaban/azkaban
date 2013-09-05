#!/bin/bash
azkaban_dir=$(dirname $0)/..

triggerport=`cat $azkaban_dir/conf/azkaban.properties | grep trigger.port | cut -d = -f 2`
echo "Shutting down current running AzkabanTriggerServer at port $triggerport"

proc=`cat $azkaban_dir/currentpid`

kill $proc

cat /dev/null > $azkaban_dir/currentpid
