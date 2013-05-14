---
layout: documents
nav: createnewjobtypes
context: ../../..
---

#Create Your Own Jobtypes
With plugin design of Azkaban job types, it is possible to extend Azkaban for various system environments. You should be able to execute any job under the same Azkaban work flow management and scheduling.

Creating new job types is often times very easy. Here are several ways one can do it:


## New Types with only Configuration Changes

One doesn't always need to write java code to create job types for end users. Often times, configuration changes of existing job types would create significantly different behavior to the end users. For example, in LinkedIn, apart from the _pig_ types, we also have _pigLi_ types that come with all the useful library jars pre-registered and imported. This way, normal users only need to provide their pig scripts, and the their own udf jars to Azkaban. The pig job should run as if it is run on the gateway machine from pig grunt. In comparison, if users are required to use the basic _pig_ job types, they will need to package all the necessary jars in the Azkaban job package, and do all the register and import by themselves, which often poses some learning curve for new pig/Azkaban users.

The same practice applies to most other job types. Admins should create or tailor job types to their specific company needs or clusters. 



## New Types Using Existing Job Types

If one needs to create a different job type, a good starting point is to see if this can be done by using an existing job type. In hadoop land, this most often means the hadoopJava type. Essentially all hadoop jobs, from the most basic mapreduce job, to pig, hive, crunch, etc, are java programs that submit jobs to hadoop clusters. It is usually straight forward to create a job type that takes user input and runs a hadoopJava job.

For exampke, one can take a look at the VoldemortBuildandPush job type. It will take in user input such as which cluster to push to, voldemort store name, etc, and runs hadoopJava job that does the work. For end users though, this is a VoldemortBuildandPush job type with which they only need to fill out the .job file to push data from hadoop to voldemort stores.

The same applies to the hive type.

## New Types by Extending Existing Ones

For the most flexibility, one can always build new types by extending the existing ones. Azkaban uses reflection to load job types that implements the _job_ interface, and tries to construct a sample object upon loading for basic testing. When executing a real job, Azkaban calls the _run_ method to run the job, and _cancel_ method to cancel it. 

For new hadoop job types, it is important to use the correct _hadoopsecuritymanager_ class, which is also included in _azkaban-plugins_ repo. This class handles talking to the hadoop cluster, and if needed, requests tokens for job execution or for name node communication. 

For better security, tokens should be requested in Azkaban main process and be written to a file. Before executing user code, the job type should implement a wrapper that picks up the token file, set it in the _Configuration_ or _JobConf_ object. Please refer to _HadoopJavaJob_ and _HadoopPigJob_ to see example usage.
