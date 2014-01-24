---
layout: documents
nav: hadoopsecuritymanager
context: ../..
---

# Support for Hadoop Security

The most common adoption of Azkaban has been in the big data platforms such as Hadoop, etc. Azkaban's jobtype plugin system allows most flexible support to such systems. 

Azkaban is able to support all Hadoop versions, with support for Hadoop security features; Azkaban is able to support various ecosystem components with all different versions, such as different versions of pig, hive, on the same instance.

A common pattern to achieve this is by using the _HadoopSecurityManager_ class, which handles talking to a Hadoop cluster and take care of Hadoop security, in a secure way.



## Hadoop Security with Kerberos, Hadoop Tokens

When Hadoop is used in enterprise production environment, it is advisable to have its security feature turned on, to protect your data and guard against mistakes.

### Kerberos Authentication

The most common authentication provided by Apache Hadoop is via Kerberos, which requires a KDC to authenticate users and services.

A user can authenticate with KDC via username/password or use a keytab. KDC distributes a tgt to authenticated users. Hadoop services, such as name node and job tracker, can use this tgt to verify this is authenticated user.

### Hadoop Tokens

Once a user is authenticated with Hadoop services, Hadoop will issue tokens to the user so that its internal services won't flood KDC. For a description of tokens, see [here](http://hortonworks.com/blog/the-role-of-delegation-tokens-in-apache-hadoop-security/).

### Hadoop SecurityManager

For human users, one authenticate with KDC with a kinit command. But for scheduler such as Azkaban that runs jobs on behalf as other users, it needs to acquire tokens that will be used by the users. Specific Azkaban job types should handle this, with the use of HadoopSecurityManager class.

For instance, when Azkaban loads the pig job type, it will initiate a HadoopSecurityManager that is authenticated with the desired KDC and Hadoop Cluster. The pig job type conf should specify which tokens are needed to talk to different services. At minimum it needs tokens from name node and job tracker. When a pig job starts, it will go to the HadoopSecurityManager to acquire all those tokens. When the user process finishes, the pig job type calls HadoopSecurityManager again to cancel all those tokens.

## Settings Common to All Hadoop Clusters

When a user program wants to talk to a Hadoop cluster, it needs to know where are the name node and job tracker. It also needs to know how to authenticate with them. These information are all in the Hadoop config files that are normally in _$HADOOP\_HOME/conf_. For this reason, this conf directory as well as hadoop-core jar need to be on azkaban executor server classpath.

If you are using Hive that uses HCat as its metastore, you also need relevant hive jars and hive conf on the classpath as well. 

### native library

Most likely your Hadoop platform depends on some native library, this should be specified in java.library.path in azkaban executor server.

### temp dir

Besides those, many tools on Hadoop, such as Pig/Hive/Crunch write files into temporary directory. By default, they all go to /tmp. This could cause operations issue when a lot of jobs run concurrently. Because of this, you may want to change this by setting _java.io.tmp.dir_ to a different directory.

## Settings To Talk to UNSECURE Hadoop Cluster

If you are just starting out with Hadoop, chances are you don't have kerberos authentication for your Hadoop. Depending on whether you want to run everything as azkaban user (or whatever user started the azkaban executor server), you can do the following settings:

a) If you started the executor server with user named azkaban, and you want to run all the jobs as azkaban on Hadoop, just set _azkaban.should.proxy=false_ and _obtain.binary.token=false_

b) If you started the executor server with user named azkaban, but you want to run Hadoop jobs as their individual users, you need to set _azkaban.should.proxy=true_ and _obtain.binary.token=false_

## Settings To Talk to SECURE Hadoop Cluster

For secure Hadoop clusters, Azkaban needs its own kerberos keytab to authenticate with KDC. Azkaban job types should acquire necessary Hadoop tokens before user job process starts, and should cancel the tokens after user job finishes. 

All job type specific settings should go to their respective plugin conf files. Some of the common settings can go to commonprivate.properties and common.properties.

For instance, Hadoop job types usually require name node tokens and job tracker tokens. These can go to commonpriate.properties.

### Azkaban as proxy user

The following settings are needed for HadoopSecurityManager to authenticate with KDC:

_proxy.user=YOUR\_AZKABAN\_KERBEROS\_PRINCIPAL_

This principal should also be set in core-site.xml in Hadoop conf with corresponding permissions.

_proxy.keytab.location=KEYTAB\_LOCATION_

One should verify if the proxy user and keytab works with the specified KDC.

### Obtaining tokens for user jobs

Here are what's common for most Hadoop jobs

_hadoop.security.manager.class=azkaban.security.HadoopSecurityManager\_H\_1\_0_

This implementation should work with Hadoop 1.x

_azkaban.should.proxy=true_

_obtain.binary.token=true_

_obtain.namenode.token=true_

_obtain.jobtracker.token=true_

Additionally, if your job needs to talk to HCat, for example if you have Hive installed with uses kerbrosed HCat, or your pig job needs to talk to HCat, you will need to set for those Hive job types

_obtain.hcat.token=true_

This makes HadoopSecurityManager acquire a HCat token as well.

## Making a New Job Type on Secure Hadoop Cluster

If you are making a new job type that will talk to Hadoop Cluster, you can use the HadoopSecurityManager to take care of security.

For unsecure Hadoop cluster, there is nothing special that is needed.

For secure Hadoop clusters, there are two ways inlcuded in the hadoopsecuritymanager package:

a) give the key tab information to user job process. The hadoopsecuritymanager static method takes care of login from that common keytab and proxy to the user. This is convenient for prototyping as there will be a real tgt granted to the user job. The con side is that the user could potentially use the keytab to login and proxy as someone else, which presents a security hole.

b) obtain Hadoop tokens prior to user job process start. The job wrapper will pick up these binary tokens inside user job process. The tokens should be explicitly cancelled after user job finishes.

By paring properly configured hadoopsecuritymanager with basic job types such as hadoopJava, pig, hive, one can make these job types work with different versions of Hadoop with various security settings.

Included in the azkaban-plugins is the hadoopsecuritymanager for Hadoop-1.x versions. It is not compatible with Hadoop-0.20 and prior versions as Hadoop UGI is not backwards compatible. However, it should not be difficult to implement one that works with them. Going forward, Hadoop UGI is mostly backwards compatible and one only needs to recompile hadoopsecuritymanager package with newer versions of Hadoop.


