#!/bin/bash

script_dir=$(dirname $0)

${script_dir}/internal/internal-start-solo-server.sh "$@" > soloServerLog__`date +%F+%T`.out 2>&1 &

exitcode=$?

if [ $exitcode -eq 0 ]
then
   echo "solo server started"
else
   echo "solo server FAILED"
fi