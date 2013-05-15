#!/bin/bash
proc=`cat currentpid`
echo "killing AzkabanSingleServer"
kill $proc

cat /dev/null > currentpid
