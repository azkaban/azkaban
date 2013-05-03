---
layout: documents
nav: jobtypeplugin
expand: plugins
context: ../..
---
#Azkaban Jobtype Plugins Configurations
These are properties to configure the jobtype plugins that are installed with the AzkabanExecutorServer. Note that Azkaban uses the directory structure to infer global settings versus individual jobtype specific settings. Sub-directory names also determine the job type name for running Azkaban instances. 

### Introduction

Jobtype plugins determine how individual jobs are actually run locally or on a remote cluster. It gives great benefits: one can add or change any job type without touching Azkaban core code; one can easily extend Azkaban to run on different hadoop versions or distributions; one can keep old versions around while adding new versions of the same types.
However, it is really up to the admin who manages these plugins to make sure they are installed and configured correctly.<br/><br/>

Upon AzkabanExecutorServer start up, Azkaban will try to load all the job type plugins it can find. Azkaban will do very simply tests and drop the bad ones. One should always try to run some test jobs to make sure the job types really work as expected.
<br/><br/>

### Global Properties

One can pass global settings to all job types, including cluster dependent settings that will be used by all job types.
These settings can also be specified in each job type's own settings as well.

## private settings
One can pass global settings that are needed by job types but should not be accessible by user code in _commonprivate.properties_. For example, the following settings are often needed for a hadoop cluster:

|{.parameter}Parameter			|{.description} Description                                                                                       							|
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
|hadoop.security.manager.class 	| The hadoopsecuritymanager that handles talking to a hadoop cluseter. Use _azkaban.security.HadoopSecurityManager\_H\_1\_0_ for 1.x versions 	|
|azkaban.should.proxy       	| Whether Azkaban should proxy as individual user hadoop accounts, or run as the Azkaban user itself, defaults to _true_ 					|
|proxy.user      				| The Azkaban user configured with kerberos and hadoop. Similar to how oozie should be configured, for secure hadoop installations 		 	|
|proxy.keytab.location  		| The location of the keytab file with which Azkaban can authenticate with Kerberos for the specified proxy.user                            |
|jobtype.global.classpath    	| The jars or xml resources every job type should have on their classpath. (e.g. $\{hadoop.home\}/hadoop\-core\-1.0.4.jar,$\{hadoop.home\}/conf)|
|jobtype.global.jvm.args 		| The jvm args that every job type should have to jvm.       																				|
|hadoop.home 					| The $HADOOP_HOME setting.      																											|
{.params}

## public settings
One can pass global settings that are needed by job types and can be visible by user code, in _common.properties_. For example, hadoop.home should normally be passed along to user programs.

### Settings for individual job types
In most cases, there is no extra settings needed for job types to work, other than variables like hadoop.home, pig.home, hive.home, etc.
However, it is also where most of the customizations come from.
For example, one can configure a two java job types with the same jar resources but with different hadoop configurations, thereby submitting pig jobs to different clusters.
One can also configure pig job with pre-registered jars and namespace imports for specific organizations.

