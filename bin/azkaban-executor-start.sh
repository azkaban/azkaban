azkaban_dir=$(dirname $0)/..
base_dir=$1
tmpdir=

if [[ -z "$tmpdir" ]]; then
echo "temp directory must be set!"
exit
fi

for file in $azkaban_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $azkaban_dir/extlib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/plugins/*/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $HADOOP_HOME/hadoop-core*.jar
do
  CLASSPATH=$CLASSPATH:$file
done

CLASSPATH=$CLASSPATH:$HADOOP_HOME/conf

echo $azkaban_dir;
echo $base_dir;
echo $CLASSPATH;

executorport=`cat conf/azkaban.properties | grep executor.port | cut -d = -f 2`
echo "Starting AzkabanExecutorServer on port $executorport ..."
serverpath=`pwd`

if [ -z $AZKABAN_OPTS ]; then
  AZKABAN_OPTS="-Xmx3G -server -Dcom.sun.management.jmxremote -Djava.io.tmpdir=$tmpdir -Dexecutorport=$executorport -Dserverpath=$serverpath"
fi

NATIVE_LIB_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native/Linux-amd64-64"

java $AZKABAN_OPTS $NATIVE_LIB_OPTS -cp $CLASSPATH azkaban.execapp.AzkabanExecutorServer -conf $base_dir/conf $@ &

echo $! > currentpid

