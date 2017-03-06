#!/usr/bin/env bash
# Shutdown script for azkaban executor server
installdir="$(dirname $0)/.."

pid=`cat ${installdir}/currentpid`
port=`cat ${installdir}/executor.port`

maxtry=5
if [ -z $pid ]; then
  echo "currentpid file doesn't exist in ${installdir}, shutdown completed"
  exit 0
fi

for try in $(seq 1 $maxtry); do
  if [ ! -z $pid ]; then
    echo "Killing Exec Server. [pid: $pid, port: $port], attempt: $try"
    kill ${pid}
    if [ -n "$(ps -p $pid -o pid=)" ]; then
      echo "Exec Server is not dead [pid: $pid, port: $port]"
      if [ $try -lt $maxtry ]; then
        echo "sleeping for a few seconds before retry"
        sleep 10
      fi
    else
      rm  ${installdir}/currentpid
      rm  ${installdir}/executor.port
      echo "shutdown succeeded"
      exit 0
    fi
  fi
done

echo "Error: Shutdown failed"
exit 1
