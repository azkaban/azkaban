---
layout: documents
nav: hdfsbrowser
expand: plugins
context: ../..
---
#Azkaban HDFS Browser Configurations
These are properties to configure the HDFS Browser on the AzkabanWebServer. They can be set in _azkaban_web_server_dir/plugins/viewer/hdfs/conf/plugin.properties_.


### Settings
|{.parameter}Parameter               |{.description} Description                                                       					|{.default}Default    |
|---------------------|-----------------------------------------------------------------------------------------------------------------|---------------------|
|viewer.name          | The name of this viewer plugin																					| HDFS               |
|viewer.path          | The path to this viewer plugin inside viewer directory.                           								| hdfs			    	|
|viewer.order         | The order of this viewer plugin amongst all viewer plugins.	                                              		| 1		             |
|viewer.hidden        | Whether this plugin should show up on the web UI.		                                                       	| false                |
|viewer.external.classpath | Extra jars this viewer plugin should load upon init.                                                        | extlib/\*			 |
|viewer.servlet.class| The main servelet class for this viewer plugin. Use _azkaban.viewer.hdfs.HdfsBrowserServlet_ for hdfs browser  	| 						|
|hadoop.security.manager.class | The class that handles talking to hadoop clusters.	Use _azkaban.security.HadoopSecurityManager\_H\_1\_0_ for hadoop 1.x |                 |
|azkaban.should.proxy          | Whether Azkaban should proxy as individual user hadoop accounts on a secure cluster, defaults to false	| false               |
|proxy.user          | The Azkaban user configured with kerberos and hadoop. Similar to how oozie should be configured, for secure hadoop installations | 			    	|
|proxy.keytab.location         | The location of the keytab file with which Azkaban can authenticate with Kerberos for the specified proxy.user			|                |
|allow.group.proxy          | Whether to allow users in the same headless user group to view hdfs filesystem as that headless user 	| false			    	|
{.params}


