---
layout: documents
nav: hivetype
context: ../../..
---

#Hive Type
Hive type is for running hive jobs. In _azkaban-plugins_ repo, we have included hive type based on hive-0.8.1. It should work for higher version hive versions as well. It is up to the admin to alias one of them as _the hive type_ for Azkaban users.

Hive type is built using hadoop tokens to talk to secure hadoop clusters. Therefore, individual Azkaban hive jobs are restricted to run within the token's lifetime, which is set by hadoop admins. It is also important that individual MR step inside a single pig script doesn't cancel the tokens upon its completion. Otherwise, all following steps will fail on authentication with job tracker or name node.

## How-To-Use

The hive job runs user hive queries. 

Hive job type talks to a secure cluster via hadoop tokens. The admin should specify "obtain.binary.token=true" if the hadoop cluster security is turned on. Before executing a job, Azkaban will obtain name node and job tracker tokens for this job. These tokens will be written to a token file, which will be picked up by user job process during its execution. After the job finishes, Azkaban takes care of canceling these tokens from name node and job tracker. <br/>

Since Azkaban only obtains the tokens at the beginning of the job run, and does not request new tokens or renew old tokens during the execution, it is important that the job does not run longer than configured token life. It is also important that individual MR step inside a single pig script doesn't cancel the tokens upon its completion. Otherwise, all following steps will fail on authentication with hadoop services.<br/>

Here are the common configurations that make a hive job for single line hive query:

|{.parameter}Parameter               |{.description} Description                                                       					|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|type		          | The type name as set by the admin, e.g. "hive"												                 	|
|azk.hive.action      | use "execute.query"															|
|hive.query      	  | Used for single line hive query.																				|
|user.to.proxy		  | The hadoop user this job should run under.  																	|
{.params}

Specify these for multi-line hive query:

|{.parameter}Parameter               |{.description} Description                                                       					|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|type		          | The type name as set by the admin, e.g. "hive"												                 											|
|azk.hive.action      | use "execute.query"															|
|hive.query.01     	  | fill in the individual hive queries, starting from 01																				|
|user.to.proxy		  | The hadoop user this job should run under.  																	|
{.params}

Specify these for query from a file:

|{.parameter}Parameter               |{.description} Description                                                       					|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|type		          | The type name as set by the admin, e.g. "hive"												                 											|
|azk.hive.action      | use "execute.query"															|
|hive.query.file   	  | location of the query file																				|
|user.to.proxy		  | The hadoop user this job should run under.  																	|
{.params}

Here are what's needed and normally configured by the admn:

these go into private.properties:

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
|hive.aux.jars.path				| Where to find auxiliary library jars																	|
|env.HADOOP\_HOME			| $HADOOP\_HOME							|
|env.HIVE\_HOME					| $HIVE\_HOME													|
|env.HIVE\_AUX\_JARS\_PATH			| $\{hive\.aux\.jars\.path\}							|
|hive.home						| $HIVE\_HOME														|
|hive.classpath.items			| Those that needs to be on hive classpath, include the conf directory							|
{.params}

these go into plugin.properties

|{.parameter}Parameter               |{.description} Description																		|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|job.class			  | azkaban.jobtype.hiveutils.azkaban.HiveViaAzkaban																|
|hive.aux.jars.path				| Where to find auxiliary library jars																	|
|env.HIVE\_HOME					| $HIVE\_HOME													|
|env.HIVE\_AUX\_JARS\_PATH			| $\{hive.aux.jars.path\}							|
|hive.home						| $HIVE\_HOME														|
|hive.jvm.args					| -Dhive.querylog.location=. -Dhive.exec.scratchdir=_YOUR\_HIVE\_SCRATCH\_DIR_	-Dhive.aux.jars.path=$\{hive.aux.jars.path\}	|
{.params}

Since hive jobs are essentially java programs, the configurations for java jobs could also be set.


## Sample Job Package

Here is a sample job package. It assumes you have hadoop installed and gets some dependency jars from $HADOOP\_HOME.
It also assumes you have hive installed and configured correctly, including setting up a mysql instance for hive metastore.
[hive.zip](https://s3.amazonaws.com/azkaban2/azkaban2/samplejobs/hive.zip) _Uploaded May 13, 2013_
