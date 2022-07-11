#!/bin/bash

azkaban_dir=$(dirname $0)/../..

# Specifies location of azkaban.properties, log4j.properties files
# Change if necessary
conf=$azkaban_dir/conf

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

executorport=`cat $conf/azkaban.properties | grep executor.port | cut -d = -f 2`
serverpath=`pwd`

if [[ -z "$AZKABAN_OPTS" ]]; then
  AZKABAN_OPTS="-Xmx4G"
fi
# Set the log4j configuration file
if [ -f $conf/log4j.xml ]; then
  AZKABAN_OPTS="$AZKABAN_OPTS -Dlog4j.configuration=file:$conf/log4j.xml -Dlog4j.log.dir=$azkaban_dir/logs"
elif [ -f $conf/log4j.properties ]; then
  AZKABAN_OPTS="$AZKABAN_OPTS -Dlog4j.configuration=file:$conf/log4j.properties -Dlog4j.log.dir=$azkaban_dir/logs"
else
  echo "Exit with error: neither $conf/log4j.xml nor $conf/log4j.properties exist."
  exit 1;
fi
AZKABAN_OPTS="$AZKABAN_OPTS -server -Dcom.sun.management.jmxremote -Djava.io.tmpdir=$tmpdir -Dexecutorport=$executorport -Dserverpath=$serverpath"

java $AZKABAN_OPTS $JAVA_LIB_PATH -cp $CLASSPATH azkaban.webapp.AzkabanWebServer -conf $conf $@ &

echo $! > $azkaban_dir/currentpid

