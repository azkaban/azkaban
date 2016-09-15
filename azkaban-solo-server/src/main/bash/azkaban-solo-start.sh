#!/bin/bash

azkaban_dir=$(dirname $0)/..

if [[ -z "$tmpdir" ]]; then
tmpdir=/tmp
fi

for file in $azkaban_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $azkaban_dir/extlib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $azkaban_dir/plugins/*/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ "$HADOOP_HOME" != "" ]; then
        echo "Using Hadoop from $HADOOP_HOME"
        CLASSPATH=$CLASSPATH:$HADOOP_HOME/conf:$HADOOP_HOME/*
        JAVA_LIB_PATH="-Djava.library.path=$HADOOP_HOME/lib/native/Linux-amd64-64"
else
        echo "Error: HADOOP_HOME is not set. Hadoop job types will not run properly."
fi

if [ "$HIVE_HOME" != "" ]; then
        echo "Using Hive from $HIVE_HOME"
        CLASSPATH=$CLASSPATH:$HIVE_HOME/conf:$HIVE_HOME/lib/*
fi

echo $azkaban_dir;
echo $CLASSPATH;

executorport=`cat $azkaban_dir/conf/azkaban.properties | grep executor.port | cut -d = -f 2`
serverpath=`pwd`

if [ -z $AZKABAN_OPTS ]; then
  AZKABAN_OPTS=-Xmx3G
fi
AZKABAN_OPTS="$AZKABAN_OPTS -server -Dcom.sun.management.jmxremote -Djava.io.tmpdir=$tmpdir -Dexecutorport=$executorport -Dserverpath=$serverpath -Dlog4j.log.dir=$azkaban_dir/logs"

java $AZKABAN_OPTS -cp $CLASSPATH azkaban.soloserver.AzkabanSingleServer -conf $azkaban_dir/conf $@ &

echo $! > $azkaban_dir/currentpid

