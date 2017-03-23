#!/bin/bash
# This script starts the solo server
source "$(dirname $0)/util.sh"

installdir=$(dirname $0)/..
abort_if_process_already_running ${installdir}

# Specifies location of azkaban.properties, log4j.properties files
# Change if necessary
conf=${installdir}/conf

if [[ -z "${tmpdir:-}" ]]; then
  tmpdir=/tmp
fi

CLASSPATH=${CLASSPATH:-}:${installdir}/lib/*:${installdir}/extlib/*

if [ "$HADOOP_HOME" != "" ]; then
  echo "Using Hadoop from $HADOOP_HOME"
  CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/conf:${HADOOP_HOME}/*
  JAVA_LIB_PATH="-Djava.library.path=$HADOOP_HOME/lib/native/Linux-amd64-64"
else
  echo "Error: HADOOP_HOME is not set. Hadoop job types will not run properly."
fi

if [ "$HIVE_HOME" != "" ]; then
  echo "Using Hive from $HIVE_HOME"
  CLASSPATH=${CLASSPATH}:${HIVE_HOME}/conf:${HIVE_HOME}/lib/*
fi

echo "CLASSPATH: ${CLASSPATH}";

executorport=`cat ${conf}/azkaban.properties | grep executor.port | cut -d = -f 2`
serverpath=`pwd`

# Set the log4j configuration file
if [ -f ${conf}/log4j.properties ]; then
  AZKABAN_OPTS="$AZKABAN_OPTS -Dlog4j.configuration=file:$conf/log4j.properties"
fi
AZKABAN_OPTS="$AZKABAN_OPTS -Xmx3G -server -Djava.io.tmpdir=$tmpdir -Dexecutorport=$executorport -Dserverpath=$serverpath -Dlog4j.log.dir=$installdir/logs"

java ${AZKABAN_OPTS} -cp ${CLASSPATH} azkaban.soloserver.AzkabanSingleServer -conf ${conf} $@ &

echo $! > ${installdir}/currentpid

