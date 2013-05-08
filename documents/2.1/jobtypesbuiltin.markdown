---
layout: documents
nav: builtinjobtypes
context: ../..
---

# Built-in Job types

Azkaban allows custom job types to be added as [plugins](./jobtypeplugin.html). However it also 
supplies several built-in job types. On top of the job parameters that can be set, each job type
has additional properties that can be used.

### Command
Command type of job can be set with _type=command_. It is a barebones command line executor. Many of the
other job types wrap the _command_ job type but constructs their own command lines.

|{.parameter}Parameter			|{.description} Description             |Required?|
|-------------------------------|---------------------------------------|---------|
|command 	| The command line string to execute. i.e. {code}ls -lh{code} | yes     |
|command. _n_ | Where _n_ is a sequence of integers (i.e 1,2,3...). Defines additional commands that run in sequential order after the initial command.| no |
{.params}

### Java Process
Java process jobs are a convenient wrapper for kicking off Java-based programs. It is equivalent to running a class with a main method from the command line. The following properties are available in javaprocess jobs: 

|{.parameter}Parameter			|{.description} Description                                              | Required? |
|-------------------------------|------------------------------------------------------------------------|-----------|
| java.class | The class that contains the main function. {code}i.e azkaban.example.text.HelloWorld{code} | yes       |
| classpath  | Comma delimited list of jars and directories to be added to the classpath. Default is all jars in the current working directory. | no |
| Xms | The initial memory pool start size. The default is 64M | no |
| Xmx | The initial maximum memory pool size. The default is 256M | no |
| main.args | A list of comma delimited arguments to pass to the java main function | no |
| jvm.args | JVM args. This entire string is passed intact as a VM argument. {code}-Dmyprop=test -Dhello=world{code} | no |
{.params}

### Noop
A job that takes no parameters and is essentially a null operation. Used for organizing your graph.

