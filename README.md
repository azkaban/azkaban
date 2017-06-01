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

### Code Style
Azkaban follows Google code style. The template file, `intellij-java-google-style.xml`, can be found in the root 
directory.

#### IntelliJ IDEA 2017.1 on Mac OS X
To install, copy `intellij-java-google-style.xml` into `~/Library/Preferences/IntelliJIdea2017.1/codestyles`

#### IntelliJ IDEA 2017.1 on Linux
To install, copy `intellij-java-google-style.xml` into `$HOME/.IdeaIC2017.1/config/codestyles`

After that, you should be able to set up the code style from `Preferences > Editor > Code Style > Scheme` 

We also leverage intellij save actions plugin(https://github.com/dubreuia/intellij-plugin-save-actions) to reformat/refactor code automatically:
It allows us to do following when saving a file:
* Organize imports
* Reformat code based on the code style
* Rearrange code (reorder methods, fields, etc.)
* Add final to local variable
* Add final to field
* Remove explicit generic type for diamond
* Qualify field access with this
* Remove unused suppress warning annotation
* Remove final from private method
* Remove unnecessary semicolon
* Add missing @Override annotations
