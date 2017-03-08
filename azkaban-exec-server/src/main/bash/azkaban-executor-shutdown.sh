#!/usr/bin/env bash
# Shutdown script for azkaban executor server
set -o nounset
set -o errexit

installdir="$(dirname $0)/.."
pid=`cat ${installdir}/currentpid`
port=`cat ${installdir}/executor.port`

maxtry=5


echo "Error: unable to kill process for $maxtry attempt(s), shutdown failed"
exit 1

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
  rm -f ${installdir}/executor.port
  exit 0
fi