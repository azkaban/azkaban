---
layout: documents
nav: voldemortbuildandpushtype
context: ../..
---

#VoldemortBuildandPush Type

Pushing data from hadoop to voldemort store used to be entirely in java. This created lots of problems, mostly due to users having to keep track of jars and dependencies and keep them up-to-date. We created the "VoldemortBuildandPush" job type to address this problem. Jars and dependencies are now managed by admins; absolutely no jars or java code are required from users.

## How-To-Use

This is essentially a hadoopJava job, with all jars controlled by the admins. User only need to provide a .job file for the job and specify all the parameters.
The following needs to be specified:

|{.parameter}Parameter               |{.description} Description                                                       					|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|type		          | The type name as set by the admin, e.g. "VoldemortBuildandPush"																							|
|push.store.name      | The voldemort push store name																|
|push.store.owners    | The push store owners					|
|push.store.description		| Push store description										 					|
|build.input.path 		| Build input path on hdfs										 					|
|build.output.dir 		| Build output path on hdfs										 					|
|build.replication.factor 		| replication factor number										 					|
|user.to.proxy		  | The hadoop user this job should run under.  																	|
|build.type.avro	  | if build and push avro data, true, otherwise, false 																						|
|avro.key.field 		| if using avro data, key field										 					|
|avro.value.field 		| if using avro data, value field										 					|
{.params}

Here are what's needed and normally configured by the admn (always put common properties in commonprivate.properties and common.properties for all job types)

These go into private.properties

|{.parameter}Parameter               |{.description} Description																		|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|hadoop.security.manager.class | The class that handles talking to hadoop clusters.														|
|azkaban.should.proxy          | Whether Azkaban should proxy as individual user hadoop accounts.										|
|proxy.user          			| The Azkaban user configured with kerberos and hadoop, for secure clusters.							|
|proxy.keytab.location         | The location of the keytab file with which Azkaban can authenticate with Kerberos for the specified proxy.user		|
|hadoop.home          			| The hadoop home where the jars and conf resources are installed.										|
|jobtype.classpath				| The items that every such job should have on its classpath.											|
|jobtype.class					| Should be set to _azkaban.jobtype.HadoopJavaJob_														|
|obtain.binary.token			| Whether Azkaban should request tokens. Set this to true for secure clusters.							|
|azkaban.no.user.classpath			| Set to true such that Azkaban doesn't pick up user supplied jars.							|
{.params}

These go into plugin.properties

|{.parameter}Parameter               |{.description} Description																		|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|job.class				| voldemort.store.readonly.mr.azkaban.VoldemortBuildAndPushJob														|
|voldemort.fetcher.protocol         | webhdfs										|
|hdfs.default.classpath.dir       			| HDFS location for distributed cache							|
|hdfs.default.classpath.dir.enable        | set to true if using distributed cache to ship dependency jars		|
{.params}

Please refer to voldemort project site for more info:
[project voldemort](http://www.project-voldemort.com/voldemort/)
