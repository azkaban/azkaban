---
layout: documents
nav: pluginsetup
expand: installation
context: ../..
---
#Setup Azkaban Plugins
Azkaban allows for pluginable viewers for the AzkabanWebServer, and pluginable executors for AzkabanExecutorServer.
A set of common plugins are available to download from the [download page](../../downloads.html).

Alternatively, by cloning the [github repo](https://github.com/azkaban/azkaban-plugins), you can run _ant -package_.

----------
### HDFS Viewer Plugins
In the plugins directory, extract the _azkaban-hdfs-viewer_ archive to the AzkabanWebServer ./plugins/viewer directory. 
Azkaban should automatically pick up the viewer plugin. Restart the Web server.

### Job Type Plugins
Azkaban allows you to extend the types of jobs you can execute. Some of the common ones that have been added are the
Hadoop, Pig and Hive type. These are included in azkaban-jobtype archive. 

Extract the archive into AzkabanExecutorServer _./plugins/jobtypes_ directory. Restart the Executor server.