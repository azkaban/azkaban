---
layout: documents
nav: javatype
context: ../../..
---

#Java Job Type
The java job type was widely used in the original Azkaban as a built-in type. It is no longer a built-in type in Azkaban2. The "javaprocess" is still built-in in Azkaban2.
The main difference between "java" and "javaprocess" job types are: 
1) _javaprocess_ runs user program that has a "main" method, _java_ runs Azkaban provided main method which invokes user program "run" method.
2) Azkaban can do the setup, such as getting Kerberos ticket or requesting hadoop tokens in the provided main in _java_ type, whereas in _javaprocess_ user is responsible for everything.

As a result, most users use _java_ type for running anything that talks to hadoop clusters. That usage should be replaced by _hadoopJava_ type now, which is secure. But we still keep _java_ type in the plugins for backwards compatibility.

## How-To-Use
Azkaban spawns a local process for the java job type that runs user programs. It is different from the "javaprocess" job type in that Azkaban already provides a "main" method, called "JavaJobRunnerMain". Inside "JavaJobRunnerMain", it looks for the "run" method which can be specified by "method.run" (default is "run"). User can also specify a cancel method in the case the user wants to gracefully terminate the job in the middle of the run.<br/>

For the most part, using _java_ type should be no different from _hadoopJava_ . 


## Sample Job

Please refer to _hadoopJava_ type.


