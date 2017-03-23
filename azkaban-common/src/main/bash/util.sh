#!/usr/bin/env bash
# Common utils
set -o nounset


# If process is still running then abort start up
function abort_if_process_already_running {
  installdir="$1"
  pidfile="${installdir}/currentpid"
  if [ -f ${pidfile} ]; then
    pid=`cat ${pidfile}`
    ps -p ${pid} 2>&1 1>nul && echo "Process already running [pid: ${pid}]. Aborting." && exit 1
  fi
}

# kill the process with retry
# return 0 if kill succeeds or no process to kill,
#        1 if kill fails
function kill_process_with_retry {
   pid="$1"
   pname="$2"
   maxattempt="$3"

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
        echo "$pname is not dead [pid: $pid]"
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

   echo "Error: unable to kill process for $maxattempt attempt(s), killing the process with -9"
   kill -9 $pid
   sleep 5
   if [[ -n "$(ps -p $pid -o pid=)" ]]; then
      echo "$pname is not dead even after kill -9 [pid: $pid]"
      return 1
   else
    echo "shutdown succeeded"
    return 0
   fi
}
