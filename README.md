# Azkaban [![Build Status](http://img.shields.io/travis/azkaban/azkaban.svg?style=flat)](https://travis-ci.org/azkaban/azkaban)

[![Join the chat at https://gitter.im/azkaban-workflow-engine/Lobby](https://badges.gitter.im/azkaban-workflow-engine/Lobby.svg)](https://gitter.im/azkaban-workflow-engine/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Building Azkaban
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

## Documentation
Documentation is available at [azkaban.github.io](http://azkaban.github.io). 
The source code for the documentation is in the [gh-pages](https://github.com/azkaban/azkaban/tree/gh-pages) branch.

For help, please visit the [Azkaban Google Group](https://groups.google.com/forum/?fromgroups#!forum/azkaban-dev).

## Developer Guide
We recommend using [IntelliJ IDEA](https://www.jetbrains.com/idea/). Azkaban is a standard [Gradle](https://gradle.org/) 
project. You can import it into your IDE using the `build.gradle` file in the root directory. For IntelliJ, choose Open 
Project from the Quick Start box or choose Open from the File menu and select the root `build.gradle` file.

See additional information in 
[the contribution guide](https://github.com/azkaban/azkaban/blob/master/CONTRIBUTING.md).
