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

#### To increment the version, create an annotated tag with the version number.
```
git tag -a {VER} -m "Impact fork release {VER}"
./gradlew publish
git push origin {VER}
```

#### Running locally:
- `./gradlew installDist`
- `cd azkaban-solo-server/build/install/azkaban-solo-server`
- `bin/start-solo.sh` 
  - Tip 1: Edit bin/start-solo.sh:5 to not pipe output to file so it instead prints to console
  - Tip 2: Edit bin/internal/internal-start-solo-server.sh:57 to `,suspend=y` if you want to attach debugger
- [http://localhost:8081](http://localhost:8081) login azkaban / azkaban
- `bin/shutdown-solo.sh` to shutdown the process

#### Basic deployment steps from local gradle for executor & web:
```
./gradlew distZip
copy azkaban-*e*-server/build/distributions/*.zip {SERVER}
```   
```
unzip azkaban-exec-server-{VER}.zip & azkaban-web-server-{VER}.zip to {TARGET}
restore from previous azkaban-executor: conf/, plugins/, projects/{LATEST} 
restore from previous azkaban-web: conf/, plugins/
symlink {TARGET}/azkaban-exec-server-{VER} and {TARGET}azkaban-web-server-{VER} to current
```
```
systemctl restart azkaban-executor & activate it localhost:12321/executor?action=activate
systemctl restart azkaban-web
```

#### Full deployment steps from Jenkins to stage/prod:
* [Deploy Azkaban](https://github.com/ImpactInc/azkaban-plugins/blob/master/deploy-azkaban.md) - commands to deploy on the server
* [Deploy both Azkaban & Plugins change](https://github.com/ImpactInc/azkaban-plugins/blob/master/deploy-azkaban-and-plugins.md) - includes commands for deploying azkaban-plugins [repo](https://github.com/ImpactInc/azkaban-plugins)
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
