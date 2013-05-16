#!/bin/bash
azkaban_dir=$(dirname $0)/..

proc=`cat $azkaban_dir/currentpid`
echo "killing AzkabanSingleServer"
kill $proc

cat /dev/null > $azkaban_dir/currentpid
