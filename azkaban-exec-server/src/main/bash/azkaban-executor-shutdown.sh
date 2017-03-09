#!/usr/bin/env bash
# Shutdown script for azkaban executor server
set -o nounset
source "$(dirname $0)/util.sh"

installdir="$(dirname $0)/.."
maxattempt=3
pid=`cat ${installdir}/currentpid`
pname="exec server"

kill_process_with_retry "${pid}" "${pname}" "${maxattempt}"

if [[ $? == 0 ]]; then
  rm -f ${installdir}/currentpid
  rm -f ${installdir}/executor.port
  exit 0
else
  exit 1
fi