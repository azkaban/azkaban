#!/usr/bin/env bash
# Shutdown script for azkaban web server
set -o nounset
set -o errexit

installdir="$(dirname $0)/.."
source "$(dirname $0)/util.sh"
maxattempt=5
pid=`cat ${installdir}/currentpid`
pname="web server"

kill_process $pid $pname $maxattempt

if [[ $? == 0 ]]; then
  rm -f ${installdir}/currentpid
fi