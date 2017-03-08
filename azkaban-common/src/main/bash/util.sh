#!/usr/bin/env bash
# Common utils
set -o nounset
set -o errexit

# kill the process with retry
# return 0 if kill succeeds or no processs to kill,
#    1 kill fails

function kill_process_with_retry {
   pid=$1
   pname=$2
   maxattempt=$3
   if [[ -z $pid ]]; then
     echo "pid doesn't exist, shutdown completed"
     return 0
   fi

   for try in $(seq 1 $maxattempt); do
     if [[ ! -z $pid ]]; then
      echo "Killing $pname. [pid: $pid], attempt: $try"
      kill ${pid}
      sleep 5
      if [[ -n "$(ps -p $pid -o pid=)" ]]; then
        echo "web server is not dead [pid: $pid]"
        if [[ $try -lt $maxattempt ]]; then
          echo "sleeping for a few seconds before retry"
          sleep 10
        fi
      else
        echo "shutdown succeeded"
        return 0
      fi
     fi
    done

   echo "Error: unable to kill process for $maxtry attempt(s), shutdown failed"
   return 1
}