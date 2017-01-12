#!/usr/bin/env bash
# Shutdown script for azkaban executor server
installdir="$(dirname $0)/.."

pid=`cat ${installdir}/currentpid`
port=`cat ${installdir}/executor.port`

echo "Killing Executor. [pid: $pid, port: $port]"

kill ${pid}
if [ $? -ne 0 ]; then
    echo "Error: Shutdown failed"
    exit 1;
fi

rm  ${installdir}/currentpid
rm  ${installdir}/executor.port

echo "done."