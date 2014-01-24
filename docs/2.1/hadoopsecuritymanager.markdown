---
layout: documents
nav: hadoopsecuritymanager
context: ../..
---
# Hadoop support via HadoopSecurityManager class

The most common usage of Azkaban has been in the big data platforms such as Hadoop, Teradata, etc. Azkaban's jobtype plugin system allows most flexible support to such systems. 
Azkaban is able to support all in-compatible Hadoop versions, with their security features turned on or not; Azkaban is able to support various ecosystem components that run on these systems with all different versions, such as different versions of pig, hive, on the same instance.
The same applies to the Hdfs viewer plugin: there could be multiple of hdfs viewers on the same web server, each capable of viewing a different Hdfs.
A common pattern to achieve this is by using the HadoopSecurityManager class, which handles talking to a Hadoop cluster.
<br/>
<br/>
In most cases, one needs to make sure new job processes are able to talk to a Hadoop cluster.
For unsecured Hadoop cluster, this is mostly by having the Hadoop conf on the individual job classpath, and the Azkaban wrapper setting the correct ugi.
For secured Hadoop clusters, there are two ways inlcuded in the hadoopsecuritymanager package:
<br/>
a) give the key tab information to user job process. The hadoopsecuritymanager static method takes care of login from that common keytab and proxy to the user. This is convenient for prototyping as there will be a real tgt granted to the user job. The con side is that the user could potentially use the keytab and proxy as someone else.
<br/>
b) only allow hadoopsecuritymanager to login in the main Azkaban process, but obtain Hadoop tokens prior to user job process start. The Azkaban wrapper will pick up these binary tokens and use them to communicate to the Hadoop cluster. It will be the user's responsibility to make sure the tokens are not cancelled or expired before all communication to Hadoop cluster is done.
<br/>
<br/>
By paring properly configured hadoopsecuritymanager with basic job types such as hadoopJava, pig, hive, one can make these job types work with different versions of Hadoop with various security settings.
<br/>
Included in the azkaban-plugins is the hadoopsecuritymanager for Hadoop-1.x versions. It is not compatible with Hadoop-0.20 and prior versions as Hadoop UGI is not backwards compatible. However, it should not be difficult to implement one that works with them. Going forward, Hadoop UGI is mostly backwards compatible and one only needs to recompile hadoopsecuritymanager package with the corresponding hadoop core jar to make it work with newer versions of Hadoop.


