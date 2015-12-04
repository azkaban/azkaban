#!/bin/bash

rm -rf ../docker-azkaban-solo/solo/lib/azkaban-soloserver-3.0.0.jar
rm -rf ../docker-azkaban-solo/solo/lib/azkaban-common-3.0.0.jar
rm -rf ../docker-azkaban-solo/solo/lib/azkaban-execserver-3.0.0.jar

cp azkaban-common/build/libs/azkaban-*-3.0.0.jar ../docker-azkaban-solo/solo/lib/
cp azkaban-execserver/build/libs/azkaban-*-3.0.0.jar ../docker-azkaban-solo/solo/lib/
cp azkaban-soloserver/build/libs/azkaban-*-3.0.0.jar ../docker-azkaban-solo/solo/lib/
