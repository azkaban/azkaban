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

echo $azkaban_dir;
echo $base_dir;
echo $CLASSPATH;

executorport=`cat conf/azkaban.properties | grep executor.port | cut -d = -f 2`
serverpath=`pwd`

if [ -z $AZKABAN_OPTS ]; then
  AZKABAN_OPTS=-Xmx3G
fi
AZKABAN_OPTS=$AZKABAN_OPTS -server -Dcom.sun.management.jmxremote -Djava.io.tmpdir=$tmpdir -Dexecutorport=$executorport -Dserverpath=$serverpath

java $AZKABAN_OPTS -cp $CLASSPATH azkaban.webapp.AzkabanWebServer -conf $base_dir/conf $@ &

echo $! > currentpid

