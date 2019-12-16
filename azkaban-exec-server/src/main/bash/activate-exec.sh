#!/bin/bash
# activate executor server

# 1 get executor port
azkaban_dir=$(dirname $0)/..
port=$(cat ${azkaban_dir}/executor.port)
echo ${port}

# 2 activate
result=$(curl -G "http://localhost:$port/executor?action=activate" && echo)

# 3 print result
echo ${result}
