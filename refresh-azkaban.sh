#!/bin/bash
gradle publishToMavenLocal copyWeb && rsync -r ./azkaban-soloserver/build/package/web ~/projects/docker-azkaban-solo/solo/ && cd ~/projects/docker-azkaban-solo/ && gradle localDev && cd -
