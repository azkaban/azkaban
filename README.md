---
---
### ImpactInc fork

ImpactInc forked this repo from [azkaban/azkaban](https://github.com/azkaban/azkaban)
tag: [3.90.0](https://github.com/azkaban/azkaban/releases/tag/3.90.0)
commit-hash: [16b9c63](https://github.com/azkaban/azkaban/commit/16b9c637cb1ba98932da7e1f69b2f93e7882b723)
date: [2020-06-01](https://github.com/azkaban/azkaban/commits?until=2020-06-01)   
*NOTE: The 4.x version is fundamentally different and not something ImpactInc wants.*

ImpactInc shall use a 3 digit minor suffix to denote versions from this point forward.   
e.g. `3.90.001, 3.90.002, etc.`

To increment the version, create an annotated tag with the version number.
```
git tag -a {VER} -m "Impact fork release {VER}"
./gradlew publish
git push upstream {VER}
```

To deploy a new version of executor and web:
```
./gradlew distZip
scp azkaban-*e*-server/build/distributions/*.zip {SERVER}:
```   
```
sudo su azkaban
unzip azkaban-exec-server-{VER}.zip -d /opt/azkaban-executor/
unzip azkaban-web-server-{VER}.zip -d /opt/azkaban-web/
restore from previous azkaban-executor: conf/, plugins/, projects/{LATEST} 
restore from previous azkaban-web: conf/, plugins/
ln -s azkaban-exec-server-{VER} /opt/azkaban-executor/current
ln -s azkaban-web-server-{VER} /opt/azkaban-web/current
```
```
sudo systemctl restart azkaban-executor
curl -G "localhost:12321/executor?action=activate" && echo
sudo systemctl restart azkaban-web
```
---   
---
# Azkaban 

[![Build Status](http://img.shields.io/travis/azkaban/azkaban.svg?style=flat)](https://travis-ci.org/azkaban/azkaban)[![codecov.io](https://codecov.io/github/azkaban/azkaban/branch/master/graph/badge.svg)](https://codecov.io/github/azkaban/azkaban)[![Join the chat at https://gitter.im/azkaban-workflow-engine/Lobby](https://badges.gitter.im/azkaban-workflow-engine/Lobby.svg)](https://gitter.im/azkaban-workflow-engine/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)[![Documentation Status](https://readthedocs.org/projects/azkaban/badge/?version=latest)](http://azkaban.readthedocs.org/en/latest/?badge=latest)


## Build
Azkaban builds use Gradle and requires Java 8 or higher.

The following set of commands run on *nix platforms like Linux, OS X.

```
# Build Azkaban
./gradlew build

# Clean the build
./gradlew clean

# Build and install distributions
./gradlew installDist

# Run tests
./gradlew test

# Build without running tests
./gradlew build -x test
```

### Build a release

Pick a release from [the release page](https://github.com/azkaban/azkaban/releases). 
Find the tag corresponding to the release.

Check out the source code corresponding to that tag.
e.g.

`
git checkout 3.30.1
`

Build 
```
./gradlew clean build
```

## Documentation

The current documentation will be deprecated soon at [azkaban.github.io](http://azkaban.github.io). 
The [new Documentation site](https://azkaban.readthedocs.io/en/latest/) is under development.
The source code for the documentation is inside `docs` directory.

For help, please visit the [Azkaban Google Group](https://groups.google.com/forum/?fromgroups#!forum/azkaban-dev).

## Developer Guide

See [the contribution guide](https://github.com/azkaban/azkaban/blob/master/CONTRIBUTING.md).

#### Documentation development

If you want to contribute to the documentation or the release tool (inside the `tools` folder), 
please make sure python3 is installed in your environment. python virtual environment is recommended to run these scripts.

To download the python3 dependencies, run 

```bash
pip3 install -r requirements.txt
```
After, enter the documentation folder `docs` and make the build by running
```bash
cd docs
make html
```


**[July, 2018]** We are actively improving our documentation. Everyone in the AZ community is 
welcome to submit a pull request to edit/fix the documentation.
