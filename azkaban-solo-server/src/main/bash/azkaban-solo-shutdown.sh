#!/usr/bin/env bash
# Shutdown script for azkaban solo server
set -o nounset
source "$(dirname $0)/util.sh"

installdir="$(dirname $0)/.."
maxattempt=3
pid=`cat ${installdir}/currentpid`
pname="solo server"

kill_process_with_retry "${pid}" "${pname}" "${maxattempt}" && rm -f ${installdir}/currentpid
