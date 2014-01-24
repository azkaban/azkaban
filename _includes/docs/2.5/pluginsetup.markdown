---
layout: documents
nav: pluginsetup
context: ../..
---
# Setup Azkaban Plugins

## Plugins
Azkaban is designed to make non-core functionalities plugin-based, so that

a) they can be selectively installed/upgraded in different environments without changing the core Azkaban, and
b) it makes Azkaban very easy to be extended for different systems.

Right now, Azkaban allows for a number of different plugins.
On web server side, there are
* viewer plugins that enable custom web pages to add features to Azkaban. Some of the known implementations include HDFS filesystem viewer, and Reportal.
* trigger plugins that enable custom triggering methods.
* user manager plugin that enables custom user authentication methods. For instance, in LinkedIn we have LDAP based user authentication.
* alerter plugins that enable different alerting methods to users, in addition to email based alerting.

On executor server side
* pluggable job type executors on AzkabanExecutorServer, such as job types for hadoop ecosystem components.

We recommend installing these plugins for the best usage of Azkaban.

A set of common plugins are available to download from the [download page](../../downloads.html).

Alternatively, by cloning the [github repo](https://github.com/azkaban/azkaban-plugins), you can run _ant -package_ in different plugin directories to create tar ball packages.

Below are instructions of how to install these plugins to work with Azkaban.

----------

## User Manager Plugins
By default, Azkaban ships with the XMLUserManager class which authenticates users based on a xml file, which is located at _conf/azkaban-users.xml_.
This is not secure and doesn't serve many users. In real production deployment, you should rely on your own user manager class that suits your need, such as a LDAP based one. The XMLUserManager can still be used for special user accounts and managing user roles. You can find examples of these two cases in the default _azkaban-users.xml_ file.

To install your own user manager class, specify in _Azkaban2-web-server-install-dir/conf/azkaban.properties :_

    user.manager.class=MyUserManagerClass

and put the containing jar in _plugins_ directory.

## Viewer Plugins
### HDFS Viewer Plugins
1) HDFS Viewer Plugin should be installed in AzkabanWebServer plugins directory, which is specified in AzkabanWebServer's config file, for example, in _Azkaban2-web-server-install-dir/conf/azkaban.properties :_
	
	viewer.plugins=hdfs
This tells Azkaban to load hdfs viewer plugin from _Azkaban2-web-server-install-dir/plugins/viewer/hdfs_

2) Extract the _azkaban-hdfs-viewer_ archive to the AzkabanWebServer ./plugins/viewer directory. 
Rename the directory to _hdfs_, as specified in step 1).

Depending on if the hadoop installation is turned on,
3a) If the hadoop installation does not have security turned on, the default config is good enough. One can simply restart AzkabanWebServer and start using the hdfs viewer.

3b) If the hadoop installation does have security turned on, the following configs should be set differently than their default values, in plugin's config file:


|{.parameter}Parameter			|{.description}Description   																								|
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------|
|azkaban.should.proxy 			| Wether Azkaban should proxy as another user to view the hdfs filesystem, rather than Azkaban itself, defaults to _true_	|
|hadoop.security.manager.class  | The security manager to be used, which handles talking to secure hadoop cluster, defaults to _azkaban.security.HadoopSecurityManager\_H\_1\_0_ (for hadoop 1.x versions)    |
|proxy.user     				| The Azkaban user configured with kerberos and hadoop. Similar to how oozie should be configured, for secure hadoop installations 																	|
|proxy.keytab.location  		| The location of the keytab file with which Azkaban can authenticate with Kerberos for the specified proxy.user			|
{.params}

For more Hadoop security related information, see [HadoopSecurityManager](./hadoopsecuritymanager.html)

## Job Type Plugins
Azkaban has a limited set of built-in job types to run local unix commands and simple java programs. In most cases, you will want to install additional job type plugins, for example, hadoopJava, Pig, Hive, VoldemortBuildAndPush, etc. Some of the common ones are included in azkaban-jobtype archive. Here is how to install:

1) Job type plugins should be installed with AzkabanExecutorServer's plugins directory, and specified in AzkabanExecutorServer's config file, for example, in _Azkaban2-exec-server-install-dir/conf/azkaban.properties :_

	azkaban.jobtype.plugin.dir=plugins/jobtypes

This tells Azkaban to load all job types from _Azkaban2-exec-server-install-dir/plugins/jobtypes_
	
2) Extract the archive into AzkabanExecutorServer _./plugins/_ directory, rename it to _jobtypes_ as specified in step 1).

The following setting is often needed when you run Hadoop Jobs:

|{.parameter}Parameter	|{.description}Description                                                                                      											|
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
|hadoop.home		 	   | Your \$HADOOP_HOME setting.																																|
|jobtype.global.classpath  | The cluster specific hadoop resources, such as hadoop-core jar, and hadoop conf (e.g. \${hadoop.home}/hadoop-core-1.0.4.jar,\${hadoop.home}/conf)    |
{.params}

Depending on if the hadoop installation is turned on, 
3a) If the hadoop installation does not have security turned on, you can likely rely on the default settings.

3b) If the Hadoop installation does have kerberos authentication turned on, you need to fill out the following hadoop settings:

|{.parameter}Parameter	|{.description}Description  																												|
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
|hadoop.security.manager.class | The security manager to be used, which handles talking to secure hadoop cluster, defaults to _azkaban.security.HadoopSecurityManager\_H\_1\_0_ (for hadoop 1.x versions)    |
|proxy.user     		| The Azkaban user configured with kerberos and hadoop. Similar to how oozie should be configured, for secure hadoop installations    																								|
|proxy.keytab.location  | The location of the keytab file with which Azkaban can authenticate with Kerberos for the specified proxy.user							|
{.params}

For more Hadoop security related information, see [HadoopSecurityManager](./hadoopsecuritymanager.html)

4) Start the executor, watch for error messages and check executor server log. For job type plugins, the executor should do minimum testing and let you know if it is properly installed.