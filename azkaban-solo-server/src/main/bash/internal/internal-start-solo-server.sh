#!/bin/bash
if [[ -z "$AZKABAN_OPTS" ]]; then
  AZKABAN_OPTS="-Xmx512M"
fi

# This script starts the solo server
set -o nounset   # exit the script if you try to use an uninitialised variable
set -o errexit   # exit the script if any statement returns a non-true return value

source "$(dirname $0)/util.sh"

installdir="$(dirname $0)/../.."
currentpidfile="${installdir}/currentpid"

if [[ -f "$currentpidfile" ]] ; then
  if is_process_running $(<$currentpidfile) ; then
    echo "Process already running [pid: $(<$currentpidfile)]. Aborting"
    exit 1
  fi
fi

# Specifies location of azkaban.properties, log4j.properties files
# Change if necessary
conf="${installdir}/conf"

if [[ -z "${tmpdir:-}" ]]; then
  tmpdir="/tmp"
fi

CLASSPATH="${CLASSPATH:-}:${installdir}/lib/*:${installdir}/extlib/*"

HADOOP_HOME=${HADOOP_HOME:-""}  # needed for set -o nounset aove

if [ "$HADOOP_HOME" != "" ]; then
  echo "Using Hadoop from $HADOOP_HOME"
  CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/conf:${HADOOP_HOME}/*:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/yarn/*"
  JAVA_LIB_PATH="-Djava.library.path=$HADOOP_HOME/lib/native/Linux-amd64-64"
else
  echo "Error: HADOOP_HOME is not set. Hadoop job types will not run properly."
fi

HIVE_HOME=${HIVE_HOME:-""}  # Needed for set -o nounset above
if [ "$HIVE_HOME" != "" ]; then
  echo "Using Hive from $HIVE_HOME"
  CLASSPATH="${CLASSPATH}:${HIVE_HOME}/conf:${HIVE_HOME}/lib/*"
fi

PIG_HOME=${PIG_HOME:-""}  # Needed for set -o nounset above
if [ "$PIG_HOME" != "" ]; then
  echo "Using Pig from $PIG_HOME"
  CLASSPATH="${CLASSPATH}:${PIG_HOME}/conf:${PIG_HOME}/lib/*"
fi

CLASSPATH=${CLASSPATH:-""}  # Needed for set -o nounset above
echo "CLASSPATH: ${CLASSPATH}";

executorport=$(grep executor.port "${conf}/azkaban.properties" | cut -d = -f 2)
serverpath=$(pwd)

AZKABAN_OPTS="$AZKABAN_OPTS -server -Djava.io.tmpdir=$tmpdir -Dexecutorport=${executorport} \
    -Dserverpath=${serverpath} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

# Set the log4j configuration file
if [ -f $conf/log4j.xml ]; then
  AZKABAN_OPTS="$AZKABAN_OPTS -Dlog4j.configuration=file:$conf/log4j.xml -Dlog4j.log.dir=$installdir/logs"
elif [ -f $conf/log4j.properties ]; then
  AZKABAN_OPTS="$AZKABAN_OPTS -Dlog4j.configuration=file:$conf/log4j.properties -Dlog4j.log.dir=$installdir/logs"
fi

java ${AZKABAN_OPTS} -cp ${CLASSPATH} azkaban.soloserver.AzkabanSingleServer -conf ${conf} $@ &

echo $! > $currentpidfile


