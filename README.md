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

If you want to contribute to the documentation or the release tool (inside the `tools` folder), 
please make sure python3 is installed in your environment. python virtual environment is recommended to run these scripts.

To download the python3 dependencies, run 

```bash
pip3 install -r requirements.txt
```


**[July, 2018]** We are actively improving our documentation. Everyone in the AZ community is 
welcome to submit a pull request to edit/fix the documentation.
