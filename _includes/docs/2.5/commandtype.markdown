---
layout: documents
nav: commandtype
context: ../..
---

#Command Job Type (built-in)
The command job type is one of the basic built-in types. It runs multiple unix commands using java processbuilder. 
Upon execution, Azkaban spawns off a process to run the command.

## How-To-Use
One can run one or multiple commands within one command job.
Here is what is needed:

|{.parameter}Parameter               |{.description} Description                                                       					|
|---------------------|-----------------------------------------------------------------------------------------------------------------|
|type		          | command																											|
|command 		  	  | The full command to run.										 												|
{.params}

For multiple commands, do it like _command\.1_, _command\.2_, etc.


## Sample Job Package
Here is a sample job package, just to show how it works.
[command.zip](https://s3.amazonaws.com/azkaban2/azkaban2/samplejobs/command.zip) _Uploaded May 13, 2013_


