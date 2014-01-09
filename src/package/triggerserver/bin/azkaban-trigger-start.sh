#!/bin/bash

azkaban_dir=$(dirname $0)/..

if [[ -z "$tmpdir" ]]; then
tmpdir=temp
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

echo $azkaban_dir;
echo $CLASSPATH;

triggerport=`cat $azkaban_dir/conf/azkaban.properties | grep trigger.port | cut -d = -f 2`
echo "Starting AzkabanTriggerServer on port $triggerport ..."
serverpath=`pwd`

if [ -z $AZKABAN_OPTS ]; then
  AZKABAN_OPTS=-Xmx3G
fi
AZKABAN_OPTS=$AZKABAN_OPTS -server -Dcom.sun.management.jmxremote -Djava.io.tmpdir=$tmpdir -Dtriggerport=$triggerport -Dserverpath=$serverpath

java $AZKABAN_OPTS -cp $CLASSPATH azkaban.triggerapp.AzkabanTriggerServer -conf $azkaban_dir/conf $@ &

echo $! > currentpid

