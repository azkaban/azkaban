#!/usr/bin/env bash

#---
# parse_args:              parses all given args and handles args that control the behavior of
#                          launching. All other unrecognized args will be sent to the process
#                          to be launched at the end.
#                          Note this function is not supposed to be run in a sub shell, otherwise
#                          the vars won't be correctly exported to the child process, which is why
#                          it does not attempt to return the value via stdout.
# args:                    the name of the var to set final command to, the original command
#---
function parse_args {
  local target_arg="$1"
  shift
  local final_command=''
  for arg in "$@"
  do
    case "$arg" in
      -f | --foreground)
      # azkaban will run as a foreground process
      export RUN_IN_FOREGROUND='true'
      ;;
      --no-fork)
      # in addition to running in foreground,
      # this shell process would be replaced by the actual java service
      # hence all signals would be directly handled by that
      export NO_FORK='true'
      export RUN_IN_FOREGROUND='true'
      ;;
      -v | --verbose)
      export VERBOSE='true'
      ;;
      *)
      final_command="${final_command} ${arg}"
      ;;
    esac
  done
  eval "${target_arg}='${final_command}'"
}

#---
# launch:                  this will modify the command such that the output of the launch script
#                          itself (not the process to be launched) will be written to the log file
#                          specified
# returns:                 the exit status of the command
# args:                    command to be executed, log file path
#---
function launch {
  local log_file="$2"
  local cmd=''
  # this will rip off args specific to launch script
  # and pass others at the end to the callee
  parse_args 'cmd' $1
  if [[ ${RUN_IN_FOREGROUND} != 'true' ]]; then
    if [[ ${VERBOSE} = 'true' ]]; then
      echo "Launching process in background..."
      echo "Execution script logs will be written to ${log_file}"
    fi
    cmd="${cmd} >${log_file} 2>&1 &"
  else
    if [[ ${VERBOSE} = 'true' ]]; then
      echo "Launching process in foreground..."
    fi
    cmd="${cmd} 2>&1"
  fi
  run ${cmd}
}

#---
# execute:                 this will modify the command such that if it's run in background,
#                          it will write the pid to a file.
# returns:                 the exit status of the command
# args:                    all arguments will be treated as the command to be executed
#---
function execute {
  local cmd="$@"
  if [[ ${VERBOSE} = 'true' ]]; then
    echo "Executing ${cmd}"
  fi
  if [[ ${RUN_IN_FOREGROUND} != 'true' ]]; then
    pid_file="${azkaban_dir}/currentpid"
    if [[ ${VERBOSE} = 'true' ]]; then
      echo "Pid will be written to $(abs_path ${pid_file})"
    fi
    cmd="${cmd} & echo $! > ${pid_file} &"
  fi
  run ${cmd}
}

#---
# run:                     runs the given command. If NO_FORK is specified,
#                          the target process will replace the current shell.
# returns:                 the exit status of the command
# args:                    all arguments will be treated as the command to be executed
#---
function run {
  local cmd="$@"
  if [[ ${NO_FORK} != 'true' ]]; then
    eval ${cmd}
  else
    exec ${cmd}
  fi
}

function abs_path {
  echo $(cd "$(dirname '$1')/.." &> /dev/null && printf "%s/%s" "$(pwd)" "${1##*/}")
}

# exporting util func specific to this file to the callee
# this is bash specific so we might need to change this later
# if we want to support general bourne shell
export -f execute
export -f abs_path
export -f run
