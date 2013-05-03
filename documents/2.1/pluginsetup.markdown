---
layout: documents
nav: pluginsetup
expand: gettingstarted
context: ../..
---
#Setup Azkaban Plugins
Azkaban is designed to make non-core functionalities plugin-based, so that
a) they can be selectively installed/upgraded in different environments without changing the core Azkaban, and
b) it makes Azkaban very easy to be extended for different systems.
Right now, Azkaban allows for pluggable viewers on AzkabanWebServer, such as the HDFS filesystem viewer, and pluggable job type executors on AzkabanExecutorServer, such as job types for hadoop ecosystem components. We recommend installing these plugins for the best usage of Azkaban.
A set of common plugins are available to download from the [download page](../../downloads.html).

Alternatively, by cloning the [github repo](https://github.com/azkaban/azkaban-plugins), you can run _ant -package_ in different plugin directories to create tar ball packages.

----------
### HDFS Viewer Plugins
1) HDFS Viewer Plugin should be installed in AzkabanWebServer plugins directory, which is specified in AzkabanWebServer's config file, for example:<br/>
	In _Azkaban2-web-server-install-dir/conf/azkaban.properties :_<br/><br/>
	_viewer.plugins=hdfs_<br/>
<br/>
This tells Azkaban to load hdfs viewer plugin from <br/><br/>
	_Azkaban2-web-server-install-dir/plugins/viewer/hdfs_<br/>
<br/>
2) Extract the _azkaban-hdfs-viewer_ archive to the AzkabanWebServer ./plugins/viewer directory. 
Rename the directory to hdfs, as specified in step 1).
<br/><br/>
Depending on if the hadoop installation is turned on, <br/>
3a) If the hadoop installation does not have security turned on, the default config is good enough. One can simply restart AzkabanWebServer and start using the hdfs viewer.<br/><br/>

3b) If the hadoop installation does have security turned on, the following configs should be set differently than their default values:


|{.parameter}Parameter			|{.description}Description   																								|
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------|
|azkaban.should.proxy 			| Wether Azkaban should proxy as another user to view the hdfs filesystem, rather than Azkaban itself, defaults to _true_	|
|hadoop.security.manager.class  | The security manager to be used, which handles talking to secure hadoop cluster, defaults to _azkaban.security.HadoopSecurityManager\_H\_1\_0_ (for hadoop 1.x versions)    |
|proxy.user     				| The Azkaban user configured with kerberos and hadoop. Similar to how oozie should be configured, for secure hadoop installations 																	|
|proxy.keytab.location  		| The location of the keytab file with which Azkaban can authenticate with Kerberos for the specified proxy.user			|
{.params}
<hr/>



### Job Type Plugins
Azkaban has a limited set of built-in job types to run local unix commands and simple java. In most cases, you will want to install additional job type plugins, for example, Hadoop, Pig, Hive, VoldemortBuildAndPush, etc. Some of the common ones are included in azkaban-jobtype archive. Here is how to install:<br/><br/>

1) Job type plugins should be installed with AzkabanExecutorServer's plugins directory, and specified in AzkabanExecutorServer's config file, for example: <br/>
	In _Azkaban2-exec-server-install-dir/conf/azkaban.properties :_<br/><br/>
	_azkaban.jobtype.plugin.dir=plugins/jobtypes_<br/><br/>

This tells Azkaban to load all job types from <br/><br/>
	_Azkaban2-exec-server-install-dir/plugins/jobtypes_<br/><br/>
	
2) Extract the archive into AzkabanExecutorServer _./plugins/_ directory, rename it to _jobtypes_ as specified in step 1).<br/><br/>

Depending on if the hadoop installation is turned on, <br/>
3a) If the hadoop installation does not have security turned on, you just need to fill out the following hadoop settings:<br/><br/>

|{.parameter}Parameter	|{.description}Description                                                                                      											|
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
|hadoop.home		 	   | Your $HADOOP_HOME setting.																																|
|jobtype.global.classpath  | The cluster specific hadoop resources, such as hadoop-core jar, and hadoop conf (e.g. $\{hadoop.home\}/hadoop\-core\-1.0.4.jar,$\{hadoop.home\}/conf)    |
{.params}


3b) If the hadoop installation does have security turned on, you just need to fill out the following hadoop settings:<br/><br/>

|{.parameter}Parameter	|{.description}Description  																												|
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
|hadoop.security.manager.class | The security manager to be used, which handles talking to secure hadoop cluster, defaults to _azkaban.security.HadoopSecurityManager\_H\_1\_0_ (for hadoop 1.x versions)    |
|proxy.user     		| The Azkaban user configured with kerberos and hadoop. Similar to how oozie should be configured, for secure hadoop installations    																								|
|proxy.keytab.location  | The location of the keytab file with which Azkaban can authenticate with Kerberos for the specified proxy.user							|
|hadoop.home		 	| Your $HADOOP_HOME setting.																												|
|jobtype.global.classpath  | The cluster specific hadoop resources, such as hadoop-core jar, and hadoop con	(e.g. $\{hadoop.home\}/hadoop\-core\-1.0.4.jar,$\{hadoop.home\}/conf)    |
{.params}


4) Start the executor, watch for error messages and check executor server log. For job type plugins, the executor should do minimum testing and let you know if it is properly installed.

