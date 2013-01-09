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

if [ -z $AZKABAN_OPTS ]; then
  AZKABAN_OPTS="-Xmx3G -server -Dcom.sun.management.jmxremote -Djava.io.tmpdir=$tmpdir"
fi

HADOOP_NATIVE_LIB="-Djava.library.path=$HADOOP_HOME/lib/native/Linux-amd64-64"

java $AZKABAN_OPTS $HADOOP_NATIVE_LIB -cp $CLASSPATH azkaban.webapp.AzkabanWebServer -conf $base_dir/conf $@ &

echo $! > currentpid

