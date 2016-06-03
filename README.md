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

Release Process
---------------

* Versions are manually created through Team City once it's merged to  (https://teamcity.amz.relateiq.com/viewType.html?buildTypeId=RIQ2014_Services_AzkabanCore&tab=artifactory).
* Then update Azkaban version on our Docker Azkaban repository (https://github.com/relateiq/docker-azkaban-solo/blob/master/build.gradle#L56)
* Manually release Azkaban to our different environments (https://teamcity.amz.relateiq.com/project.html?projectId=RIQ2014_Services_Azkaban&tab=projectOverview)


EMR Bootstrap Process
---------------------

To learn more about our custom EMR bootstrap process, click [here](https://github.com/relateiq/MarketingCloud/wiki/Runbook#emr).

Documentation
-------------

For Azkaban documentation, please go to [Azkaban Project Site](http://azkaban.github.io). The source code for the documentation is in the [gh-pages branch](https://github.com/azkaban/azkaban/tree/gh-pages).

For help, please visit the Azkaban Google Group: [Azkaban Group](https://groups.google.com/forum/?fromgroups#!forum/azkaban-dev)
