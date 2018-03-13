#!/usr/bin/env bash
# Common utils
set -o nounset   # exit the script if you try to use an uninitialised variable
set -o errexit   # exit the script if any statement returns a non-true return value

#---
# is_process_running: Checks if a process is running
# args:               Process ID of running proccess
# returns:            returns 0 if process is running, 1 if not found
#---
function is_process_running {
  local  pid=$1
  kill -0 $pid > /dev/null 2>&1 #exit code ($?) is 0 if pid is running, 1 if not running
  local  status=$?              #because we are returning exit code, can use with if & no [ bracket
  return $status
}

#---
# args:               Process name of a running process to shutdown, install directory
# returns:            returns 0 if success, 1 otherwise
#---
function common_shutdown {
  process_name="$1"
  install_dir="$2"
  max_attempt=3
  pid=`cat ${install_dir}/currentpid`

  kill_process_with_retry "${pid}" "${process_name}" "${max_attempt}"

  if [[ $? == 0 ]]; then
    rm -f ${install_dir}/currentpid
    return 0
  else
    return 1
  fi
}

#---
# kill_process_with_retry: Checks and attempts to kill the running process
# args:                    PID, process name, number of kill attempts
# returns:                 returns 0 if kill succeds or nothing to kill, 1 if kill fails
# exception:               If passed a non-existant pid, function will forcefully exit
#---
function kill_process_with_retry {
   local pid="$1"
   local pname="$2"
   local maxattempt="$3"
   local sleeptime=5

   if ! is_process_running $pid ; then
     echo "ERROR: process name ${pname} with pid: ${pid} not found"
     exit 1
   fi

   for try in $(seq 1 $maxattempt); do
      echo "Killing $pname. [pid: $pid], attempt: $try"
      kill ${pid}
      sleep 5
      if is_process_running $pid; then
        echo "$pname is not dead [pid: $pid]"
        echo "sleeping for $sleeptime seconds before retry"
        sleep $sleeptime
      else
        echo "shutdown succeeded"
        return 0
      fi
   done

   echo "Error: unable to kill process for $maxattempt attempt(s), killing the process with -9"
   kill -9 $pid
   sleep $sleeptime

   if is_process_running $pid; then
      echo "$pname is not dead even after kill -9 [pid: $pid]"
      return 1
   else
    echo "shutdown succeeded"
    return 0
   fi
}

