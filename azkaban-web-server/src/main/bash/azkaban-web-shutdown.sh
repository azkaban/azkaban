#!/usr/bin/env bash
# Shutdown script for azkaban web server
installdir="$(dirname $0)/.."

pid=`cat ${installdir}/currentpid`
echo "Killing Web Server. [pid: $pid]"

kill ${pid}
if [ $? -ne 0 ]; then
    echo "Error: Shutdown failed"
    exit 1;
fi

rm  ${installdir}/currentpid
echo "done."
