Azkaban 3 [![Build Status](http://img.shields.io/travis/azkaban/azkaban.svg?style=flat)](https://travis-ci.org/azkaban/azkaban)
========

[![Join the chat at https://gitter.im/azkaban-workflow-engine/Lobby](https://badges.gitter.im/azkaban-workflow-engine/Lobby.svg)](https://gitter.im/azkaban-workflow-engine/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Building from Source
--------------------

To build Azkaban packages from source, run:

```
./gradlew distTar
```

The above command builds all Azkaban packages and packages them into GZipped Tar archives. To build Zip archives, run:

```
./gradlew distZip
```

If not building for the first time, it's good to clean first:

```
./gradlew clean
```

Documentation
-------------

For Azkaban documentation, please go to [Azkaban Project Site](http://azkaban.github.io). The source code for the documentation is in the [gh-pages branch](https://github.com/azkaban/azkaban/tree/gh-pages).

For help, please visit the Azkaban Google Group: [Azkaban Group](https://groups.google.com/forum/?fromgroups#!forum/azkaban-dev)
