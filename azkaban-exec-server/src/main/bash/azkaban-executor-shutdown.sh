#!/usr/bin/env bash
# Shutdown script for azkaban executor server
set -o nounset
set -o errexit
source "$(dirname $0)/util.sh"

installdir="$(dirname $0)/.."
maxattempt=5
pid=`cat ${installdir}/currentpid`
pname="exec server"

kill_process $pid $pname $maxattempt

if [[ $? == 0 ]]; then
  rm -f ${installdir}/currentpid
  rm -f ${installdir}/executor.port
  exit 0
else
  exit 1
fi