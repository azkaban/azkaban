#!/bin/bash

executorport=`cat conf/azkaban.properties | grep executor.port | cut -d = -f 2`
echo "Shutting down current running AzkabanExecutorServer at port $executorport"

proc=`cat currentpid`

kill $proc

cat /dev/null > currentpid
