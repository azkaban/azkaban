#!/usr/bin/env bash
# Shutdown script for azkaban web server
installdir="$(dirname $0)/.."

#currdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#installdir=$(dirname "${currdir}")
maxtry=5
pid=`cat ${installdir}/currentpid`

if [ -z $pid ]; then
  echo "currentpid file doesn't exist in ${installdir}, shutdown completed"
  exit 0
fi

for try in $(seq 1 $maxtry); do
  if [ ! -z $pid ]; then
    echo "Killing Web Server. [pid: $pid], $try th try"
    kill ${pid}
    if [ -n "$(ps -p $pid -o pid=)" ]; then
      echo "web server is not dead [pid: $pid]"
      if [ $try -lt $maxtry ]; then
        echo "sleeping for a few seconds before retry"
        sleep 10 
      fi
    else 
      rm  ${installdir}/currentpid
      echo "shutdown succeeded"
      exit 0
    fi
  fi
done

echo "Error: Shutdown failed"
exit 1
