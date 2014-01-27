echo "Looking for orphan processes from azkaban"
ps -ef | awk '{if ($3 == 1) {print $0}}' | grep -v Azkaban | grep azkaban

echo "Killing orphan processes"
for i in `ps -ef | awk '{if ($3 == 1) {print $0}}' | grep -v Azkaban | grep azkaban | awk '{print $2}'` ;
do
    kill -9 $i
done

