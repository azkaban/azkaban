---
layout: documents
nav: hadoopjavatype
context: ../../..
---

#hadoopJava Type
In large part, this is the same "java" type. The difference is its ability to talk to a hadoop cluster securely, via hadoop tokens. Most hadoop job types can be created by running a hadoopJava job, such as pig, hive, etc.


## How-To-Use

hadoopJava type runs user java program after all. Upon execution, it tries to construct an object that has the constructor signature of _constructor\(String, Props\)_ 

and runs its method of _run\(\)_. If user wants to cancel the job, it tries the user defined _cancel\(\)_ method before doing a hard kill on that process.

hadoopJava job type talks to a secure cluster via hadoop tokens. The admin should specify "obtain.binary.token=true" if the hadoop cluster security is turned on. Before executing a job, Azkaban will obtain name node token and job tracker tokens for this job. These tokens will be written to a token file, to be picked up by user job process during its execution. After the job finishes, Azkaban takes care of canceling these tokens from name node and job tracker. <br/>

Since Azkaban only obtains the tokens at the beginning of the job run, and does not requesting new tokens or renew old tokens during the execution, it is important that the job does not run longer than configured token life. <br/>

If there are multiple job submissions inside the user program, the user should also take care not to have a single MR step cancel the tokens upon completion, thereby failing all other MR steps when they try to authenticate with hadoop services. <br/>

In many cases, it is also necessary to add the following code to make sure user program picks up the hadoop tokens in "conf" or "jobconf" like the following:<br/>

<pre class="code">
// Suppose this is how one gets the conf
Configuration conf = new Configuration();

if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
    conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
}
</pre>

Here are some common configurations that make a hadoopJava job for a user:

|{.parameter}Parameter               |{.description} Description                                                       					|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|type		          | The type name as set by the admin, e.g. "hadoopJava"															|
|job.class            | The fully qualified name of the user job class.																	|
|classpath        	  | The resources that should be on the execution classpath, accessible to the local filesystem.					|
|main.args	          | Main arguments passed to user program.               	 														|
|dependencies 		  | The other jobs in the flow this job is dependent upon.										 					|
|user.to.proxy		  | The hadoop user this job should run under.  																	|
|method.run			  | The run method, defaults to _run\(\)_																			|
|method.cancel		  | The cancel method, defaults to _cancel\(\)_																		|
|getJobGeneratedProperties | The method user should implement if the output properties should be picked up and passed to the next job.	|
|jvm.args			  | The "-D" for the new jvm process																				|
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

Since Azkaban job types are named by their directory names, the admin should also make those naming public and consistent.


## Sample Job Package
Here is a sample job package that does a word count. It relies on a pig job to first upload the text file onto HDFS. One can also manually upload a file and run the word count program alone.
The source code is in _azkaban-plugins/plugins/jobtype/src/azkaban/jobtype/examples/java/WordCount.java_
[java-wc.zip](https://s3.amazonaws.com/azkaban2/azkaban2/samplejobs/java-wc.zip) _Uploaded May 13, 2013_


