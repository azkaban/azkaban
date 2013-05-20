---
layout: documents
nav: pigtype
context: ../..
---

#pig Type
Pig type is for running pig jobs. In _azkaban-plugins_ repo, we have included pig types from pig-0.9.2 to pig-0.11.0. It is up to the admin to alias one of them as _the pig type_ for Azkaban users.

Pig type is built on using hadoop tokens to talk to secure hadoop clusters. Therefore, individual Azkaban pig jobs are restricted to run within the token's lifetime, which is set by hadoop admins. It is also important that individual MR step inside a single pig script doesn't cancel the tokens upon its completion. Otherwise, all following steps will fail on authentication with job tracker or name node.

Vanilla pig types don't provide all udf jars. It is often up to the admin who sets up Azkaban to provide a pre-configured pig job type with company specific udfs registered and name space imported, so that the users don't need to provide all the jars and do the configurations in their specific pig job conf files.


## How-To-Use

The pig job runs user pig scripts. It is important to remember, however, that running any pig script might require a number of dependency libraries that need to be placed on local Azkaban job classpath, or be registered with pig and carried remotely, or both. By using classpath settings, as well as _pig\_additional\_jars_ and _udf\_import\_list_, the admin can create a pig job type that has very different default behavior than the most basic "pig" type.

pig job type talks to a secure cluster via hadoop tokens. The admin should specify "obtain.binary.token=true" if the hadoop cluster security is turned on. Before executing a job, Azkaban will obtain name node and job tracker tokens for this job. These tokens will be written to a token file, which will be picked up by user job process during its execution. After the job finishes, Azkaban takes care of canceling these tokens from name node and job tracker. <br/>

Since Azkaban only obtains the tokens at the beginning of the job run, and does not request new tokens or renew old tokens during the execution, it is important that the job does not run longer than configured token life. It is also important that individual MR step inside a single pig script doesn't cancel the tokens upon its completion. Otherwise, all following steps will fail on authentication with hadoop services.<br/>

Here are the common configurations that make a pig job for a _user_:

|{.parameter}Parameter               |{.description} Description                                                       					|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|type		          | The type name as set by the admin, e.g. "pig"																							|
|pig.script           | The pig script locatipn. e.g. src/wordcountpig.pig																|
|classpath        	  | The resources that should be on the execution classpath, accessible to the local filesystem.					|
|dependencies 		  | The other jobs in the flow this job is dependent upon.										 					|
|user.to.proxy		  | The hadoop user this job should run under.  																	|
|param.SOME\_PARAM	  | Equivalent to pig's -param 																						|
{.params}

Here are what's needed and normally configured by the admn:

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
{.params}

Since pig jobs are essentially java programs, the configurations for java jobs could also be set.

Since Azkaban job types are named by their directory names, the admin should also make those naming public and consistent. For example, while there are multiple versions of pig job types, the admin can link one of them as _pig_ for default pig type. Experimental pig versions can be tested in parallel with a different name and can be promoted to default pig type if it is proven stable. In LinkedIn, we also provide pig job types that have a number of useful udf libraries, including datafu and LinkedIn specific ones, pre-registered and imported, so that users in most cases will only need pig scripts in their Azkaban job packages.


## Sample Job Package

Here is a sample job package that does word count. It assumes you have hadoop installed and gets some dependency jars from $HADOOP\_HOME.
[pig-wc.zip](https://s3.amazonaws.com/azkaban2/azkaban2/samplejobs/pig-wc.zip) _Uploaded May 13, 2013_
