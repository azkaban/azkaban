#!/bin/bash
proc=`cat currentpid`
echo "killing AzkabanWebServer"
kill $proc

cat /dev/null > currentpid
