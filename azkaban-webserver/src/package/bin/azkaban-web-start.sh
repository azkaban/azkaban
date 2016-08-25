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

if [ -n "$HADOOP_HOME" ] && [ -d "$HADOOP_HOME" ]; then
  echo "Using Hadoop from $HADOOP_HOME"
  CLASSPATH=$CLASSPATH:$HADOOP_HOME/conf:$HADOOP_HOME/*
  if [ -d "$HADOOP_HOME/lib/native/Linux-amd64-64" ]; then
    JAVA_LIB_PATH="-Djava.library.path=$HADOOP_HOME/lib/native/Linux-amd64-64"
  elif [ -n "$HADOOP_MR1_HOME" ] && [ -d "$HADOOP_MR1_HOME/lib/native/Linux-amd64-64" ]; then
    JAVA_LIB_PATH="-Djava.library.path=$HADOOP_MR1_HOME/lib/native/Linux-amd64-64"
  fi
else
        echo "Error: HADOOP_HOME is not set or doesn't exist. Hadoop job types will not run properly."
fi

if [ -n "$HADOOP_HDFS_HOME" ] && [ -n "$HADOOP_HDFS_HOME" ]; then
  echo "Picking HDFS jars from $HADOOP_HDFS_HOME"
  CLASSPATH=$CLASSPATH:$HADOOP_HDFS_HOME/*
fi

if [ -n "$HADOOP_CONF_DIR" ] && [ -n "$HADOOP_CONF_DIR" ]; then
  echo "Picking up hadoop configuration from $HADOOP_CONF_DIR"
  CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR
fi

if [ "HIVE_HOME" != "" ]; then
  echo "Using Hive from $HIVE_HOME"
  CLASSPATH=$CLASSPATH:$HIVE_HOME/conf:$HIVE_HOME/lib/*
fi

if [ -n "HIVE_CONF_DIR" ] && [ -d "HIVE_CONF_DIR" ]; then
  echo "Using Hive configuration from $HIVE_CONF_DIR"
  CLASSPATH=$CLASSPATH:$HIVE_CONF_DIR
fi

echo $azkaban_dir;
echo $CLASSPATH;

executorport=`cat $azkaban_dir/conf/azkaban.properties | grep executor.port | cut -d = -f 2`
serverpath=`pwd`

if [ -z $AZKABAN_OPTS ]; then
  AZKABAN_OPTS="-Xmx4G"
fi
AZKABAN_OPTS="$AZKABAN_OPTS -server -Dcom.sun.management.jmxremote -Djava.io.tmpdir=$tmpdir -Dexecutorport=$executorport -Dserverpath=$serverpath -Dlog4j.log.dir=$azkaban_dir/logs"

java $AZKABAN_OPTS $JAVA_LIB_PATH -cp $CLASSPATH azkaban.webapp.AzkabanWebServer -conf $azkaban_dir/conf $@ &

echo $! > $azkaban_dir/currentpid

