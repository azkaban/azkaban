---
layout: documents
nav: jobconf
context: ../..
---

# Job Configurations

### Common Parameters
Besides the __type__ and the __dependencies__ parameters, there are several parameters that
Azkaban reserves for all jobs. All of the parameters below are optional.

|{.parameter}Parameter			|{.description} Description                                 |
|-------------------------------|-----------------------------------------------------------|
|retries 	| The number of retries that will be automatically attempted for failed jobs 	|
|retry.backoff  | The millisec time between each retry attempt |
|working.dir  | Override the working directory for the execution. This is by default the directory that contains the job file that is being run. |
|env. _property_ | Set the environment variable with named _property_ |
|failure.emails  | Comma delimited list of emails to notify during a failure. \* |
|success.emails  | Comma delimited list of emails to notify during a success. \* |
|notify.emails  | Comma delimited list of emails to notify during either a success or failure. \* |
{.params}

\* note that for email properties, this property is retrieved from the last job in the flow and applied flow level. All other email properties 
of jobs in the flow are ignored.


### Runtime Properties
These properties are automatically added to Azkaban properties during runtime for a job to use.

|{.parameter}Parameter			|{.description} Description                                                                                       							|
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
|azkaban.job.attempt 	| The attempt number for the job. Starts with attempt 0 and increments with every retry. |
|azkaban.flow.flowid | The flow name that the job is running in. |
|azkaban.flow.execid  | The execution id that is assigned to the running flow. |
|azkaban.flow.projectid  | The numerical project id. |
|azkaban.flow.projectversion  | The project upload version. |
|azkaban.flow.uuid  | A unique identifier assigned to a flow's execution. |
|azkaban.flow.start.timestamp | The millisecs since epoch start time. |
|azkaban.flow.start.year| The start year. |
|azkaban.flow.start.month| The start month of the year (1-12) |
|azkaban.flow.start.day| The start day of the month. |
|azkaban.flow.start.hour| The start hour in the day. |
|azkaban.flow.start.minute| The start minute.|
|azkaban.flow.start.second| The start second in the minute. |
|azkaban.flow.start.milliseconds| The start millisec in the sec|
|azkaban.flow.start.timezone| The start timezone that is set.|
{.params}

### Inherited Parameters
Any included _.properties_ files will be treated as properties that are shared amongst the individual jobs of the flow.
The properties are resolved in a hierarchical manner by directory.

For instance, suppose you have the following directory structure in your zip file.

<pre class="code">
system.properties
baz.job
myflow/
   myflow.properties
   myflow2.properties
   foo.job
   bar.job
</pre>

That directory structure will be preserved when running in Azkaban. The _baz_ job will inherit only from
_system.properties_. The jobs _foo_ and _bar_ will inherit from _myflow.properties_ and _myflow2.properties_, 
which in turn will inherit from _system.properties_.

The hierarchical ordering of properties in the same directory is arbitrary.

### Parameter Substitution
Azkaban allows for replacing of parameters. Whenever a $\{_parameter_\} is found in a properties or job file,
Azkaban will attempt to replace that parameter. The resolution of the parameters is done late.

<pre class="code">
# shared.properties
replaceparameter=bar
</pre>

<pre class="code">
# myjob.job
param1=mytest
foo=${replaceparameter}

param2=${param1}
</pre>

In the previous example, before _myjob_ is run, _foo_ will equal _bar_ and _param2_ will equal _mytest_.

Parameter substitution is also recursive, allowing for constructs such as:

<pre class="code">
# global.properties
mycluster.environment=production

# shared.properties
production.database_host=db.example.com
staging.database_host=some_other_host.example.com
</pre>

<pre class="code">
# myjob.job
database=${${mycluster.environment}.database_host}
</pre>

In this example, _myjob_ will use the value _db.example.com_, having first evaluated _mycluster.environment_ to be
_production_ and then _production.database\_host_.

### Parameter Passing
There is often a desire to pass these parameters to the executing job code. The method of passing these parameters
is dependent on the jobtype that is run, but usually Azkaban writes these parameters to a temporary file that is
readable by the job.
The path of the file is set in _JOB\_PROP\_FILE_ environment variable. The format is the same key value pair property files.
Certain built-in job types do this automatically for you. The _java_ type, for instance, will invoke your Runnable
and given a proper constructor, Azkaban can pass parameters to your code automatically.

### Parameter Output
Properties can be exported to be passed to its dependencies. A second environment variable _JOB\_OUTPUT\_PROP\_FILE_
is set by Azkaban. If a job writes a file to that path, Azkaban will read this file and then pass the output to the
next jobs in the flow.

The output file should be in json format. Certain built-in job types can handle this automatically, such as the _java_ type.




