.. _Jobtypes:

Jobtypes
==================================

Azkaban job type plugin design provides great flexibility for developers
to create any type of job executors which can work with essentially all
types of systems -- all managed and triggered by the core Azkaban work
flow management.

Here we provide a common set of plugins that should be useful to most
hadoop related use cases, as well as sample job packages. Most of these
job types are being used in LinkedIn's production clusters, only with
different configurations. We also give a simple guide how one can create
new job types, either from scratch or by extending the old ones.

--------------

*****
Command Job Type (built-in)
*****

The command job type is one of the basic built-in types. It runs
multiple UNIX commands using java processbuilder. Upon execution,
Azkaban spawns off a process to run the command.

How To Use
~~~~~~~~~~
One can run one or multiple commands within one command job. Here is
what is needed:

+---------+-------------------------+
| Type    | Command                 |
+=========+=========================+
| command | The full command to run |
+---------+-------------------------+

For multiple commands, do it like ``command.1, command.2``, etc.

.. raw:: html

   <div class="bs-callout bs-callout-info">

Sample Job Package
~~~~~~~~~~~~~~~~~~

Here is a sample job package, just to show how it works:

`Download
command.zip <https://s3.amazonaws.com/azkaban2/azkaban2/samplejobs/command.zip>`__
(Uploaded May 13, 2013)

..
   Todo:: Re-Link this

.. raw:: html

   </div>

--------------

*****
HadoopShell Job Type
*****

In large part, this is the same ``Command`` type. The difference is its
ability to talk to a Hadoop cluster securely, via Hadoop tokens.

The HadoopShell job type is one of the basic built-in types. It runs
multiple UNIX commands using java processbuilder. Upon execution,
Azkaban spawns off a process to run the command.


How To Use
~~~~~~~~~~

The ``HadoopShell`` job type talks to a secure cluster via Hadoop
tokens. The admin should specify ``obtain.binary.token=true`` if the
Hadoop cluster security is turned on. Before executing a job, Azkaban
will obtain name node token and job tracker tokens for this job. These
tokens will be written to a token file, to be picked up by user job
process during its execution. After the job finishes, Azkaban takes care
of canceling these tokens from name node and job tracker.

Since Azkaban only obtains the tokens at the beginning of the job run,
and does not requesting new tokens or renew old tokens during the
execution, it is important that the job does not run longer than
configured token life.

One can run one or multiple commands within one command job. Here is
what is needed:

+---------+-------------------------+
| Type    | Command                 |
+=========+=========================+
| command | The full command to run |
+---------+-------------------------+

For multiple commands, do it like ``command.1, command.2``, etc.

Here are some common configurations that make a ``hadoopShell`` job for
a user:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| type                              | The type name as set by the       |
|                                   | admin, e.g. ``hadoopShell``       |
+-----------------------------------+-----------------------------------+
| dependencies                      | The other jobs in the flow this   |
|                                   | job is dependent upon.            |
+-----------------------------------+-----------------------------------+
| user.to.proxy                     | The Hadoop user this job should   |
|                                   | run under.                        |
+-----------------------------------+-----------------------------------+
| hadoop-inject.FOO                 | FOO is automatically added to the |
|                                   | Configuration of any Hadoop job   |
|                                   | launched.                         |
+-----------------------------------+-----------------------------------+

Here are what's needed and normally configured by the admin:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| hadoop.security.manager.class     | The class that handles talking to |
|                                   | Hadoop clusters.                  |
+-----------------------------------+-----------------------------------+
| azkaban.should.proxy              | Whether Azkaban should proxy as   |
|                                   | individual user Hadoop accounts.  |
+-----------------------------------+-----------------------------------+
| proxy.user                        | The Azkaban user configured with  |
|                                   | kerberos and Hadoop, for secure   |
|                                   | clusters.                         |
+-----------------------------------+-----------------------------------+
| proxy.keytab.location             | The location of the keytab file   |
|                                   | with which Azkaban can            |
|                                   | authenticate with Kerberos for    |
|                                   | the specified proxy.user          |
+-----------------------------------+-----------------------------------+
| obtain.binary.token               | Whether Azkaban should request    |
|                                   | tokens. Set this to true for      |
|                                   | secure clusters.                  |
+-----------------------------------+-----------------------------------+

--------------

*****
Java Job Type
*****

The ``java`` job type was widely used in the original Azkaban as a
built-in type. It is no longer a built-in type in Azkaban2. The
``javaprocess`` is still built-in in Azkaban2. The main difference
between ``java`` and ``javaprocess`` job types are:

#. ``javaprocess`` runs user program that has a "main" method, ``java``
   runs Azkaban provided main method which invokes user program "run"
   method.
#. Azkaban can do the setup, such as getting Kerberos ticket or
   requesting Hadoop tokens in the provided main in ``java`` type,
   whereas in ``javaprocess`` user is responsible for everything.

As a result, most users use ``java`` type for running anything that
talks to Hadoop clusters. That usage should be replaced by
``hadoopJava`` type now, which is secure. But we still keep ``java``
type in the plugins for backwards compatibility.

How to Use
~~~~~~~~~~

Azkaban spawns a local process for the java job type that runs user
programs. It is different from the "javaprocess" job type in that
Azkaban already provides a ``main`` method, called
``JavaJobRunnerMain``. Inside ``JavaJobRunnerMain``, it looks for the
``run`` method which can be specified by ``method.run`` (default is
``run``). User can also specify a ``cancel`` method in the case the user
wants to gracefully terminate the job in the middle of the run.

For the most part, using ``java`` type should be no different from
``hadoopJava``.

.. raw:: html

   <div class="bs-callout bs-callout-info">

Sample Job
~~~~~~~~~~

Please refer to the  `hadoopJava type <#hadoopjava-type>`_.

.. raw:: html

   </div>

--------------

*****
hadoopJava Type
*****


In large part, this is the same ``java`` type. The difference is its
ability to talk to a Hadoop cluster securely, via Hadoop tokens. Most
Hadoop job types can be created by running a hadoopJava job, such as
Pig, Hive, etc.

How To Use
~~~~~~~~~~


The ``hadoopJava`` type runs user java program after all. Upon
execution, it tries to construct an object that has the constructor
signature of ``constructor(String, Props)`` and runs its ``run`` method.
If user wants to cancel the job, it tries the user defined ``cancel``
method before doing a hard kill on that process.

The ``hadoopJava`` job type talks to a secure cluster via Hadoop tokens.
The admin should specify ``obtain.binary.token=true`` if the Hadoop
cluster security is turned on. Before executing a job, Azkaban will
obtain name node token and job tracker tokens for this job. These tokens
will be written to a token file, to be picked up by user job process
during its execution. After the job finishes, Azkaban takes care of
canceling these tokens from name node and job tracker.

Since Azkaban only obtains the tokens at the beginning of the job run,
and does not requesting new tokens or renew old tokens during the
execution, it is important that the job does not run longer than
configured token life.

If there are multiple job submissions inside the user program, the user
should also take care not to have a single MR step cancel the tokens
upon completion, thereby failing all other MR steps when they try to
authenticate with Hadoop services.

In many cases, it is also necessary to add the following code to make
sure user program picks up the Hadoop tokens in "conf" or "jobconf" like
the following:

.. code-block:: guess

   // Suppose this is how one gets the conf
   Configuration conf = new Configuration();

   if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
       conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
   }

Here are some common configurations that make a ``hadoopJava`` job for a
user:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| type                              | The type name as set by the       |
|                                   | admin, e.g. ``hadoopJava``        |
+-----------------------------------+-----------------------------------+
| job.class                         | The fully qualified name of the   |
|                                   | user job class.                   |
+-----------------------------------+-----------------------------------+
| classpath                         | The resources that should be on   |
|                                   | the execution classpath,          |
|                                   | accessible to the local           |
|                                   | filesystem.                       |
+-----------------------------------+-----------------------------------+
| main.args                         | Main arguments passed to user     |
|                                   | program.                          |
+-----------------------------------+-----------------------------------+
| dependencies                      | The other jobs in the flow this   |
|                                   | job is dependent upon.            |
+-----------------------------------+-----------------------------------+
| user.to.proxy                     | The Hadoop user this job should   |
|                                   | run under.                        |
+-----------------------------------+-----------------------------------+
| method.run                        | The run method, defaults to       |
|                                   | *run()*                           |
+-----------------------------------+-----------------------------------+
| method.cancel                     | The cancel method, defaults to    |
|                                   | *cancel()*                        |
+-----------------------------------+-----------------------------------+
| getJobGeneratedProperties         | The method user should implement  |
|                                   | if the output properties should   |
|                                   | be picked up and passed to the    |
|                                   | next job.                         |
+-----------------------------------+-----------------------------------+
| jvm.args                          | The ``-D`` for the new jvm        |
|                                   | process                           |
+-----------------------------------+-----------------------------------+
| hadoop-inject.FOO                 | FOO is automatically added to the |
|                                   | Configuration of any Hadoop job   |
|                                   | launched.                         |
+-----------------------------------+-----------------------------------+

Here are what's needed and normally configured by the admin:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| hadoop.security.manager.class     | The class that handles talking to |
|                                   | Hadoop clusters.                  |
+-----------------------------------+-----------------------------------+
| azkaban.should.proxy              | Whether Azkaban should proxy as   |
|                                   | individual user Hadoop accounts.  |
+-----------------------------------+-----------------------------------+
| proxy.user                        | The Azkaban user configured with  |
|                                   | kerberos and Hadoop, for secure   |
|                                   | clusters.                         |
+-----------------------------------+-----------------------------------+
| proxy.keytab.location             | The location of the keytab file   |
|                                   | with which Azkaban can            |
|                                   | authenticate with Kerberos for    |
|                                   | the specified proxy.user          |
+-----------------------------------+-----------------------------------+
| hadoop.home                       | The Hadoop home where the jars    |
|                                   | and conf resources are installed. |
+-----------------------------------+-----------------------------------+
| jobtype.classpath                 | The items that every such job     |
|                                   | should have on its classpath.     |
+-----------------------------------+-----------------------------------+
| jobtype.class                     | Should be set to                  |
|                                   | ``azkaban.jobtype.HadoopJavaJob`` |
+-----------------------------------+-----------------------------------+
| obtain.binary.token               | Whether Azkaban should request    |
|                                   | tokens. Set this to true for      |
|                                   | secure clusters.                  |
+-----------------------------------+-----------------------------------+

Since Azkaban job types are named by their directory names, the admin
should also make those naming public and consistent.

.. raw:: html

   <div class="bs-callout bs-callout-info">
Sample Job Package
~~~~~~~~~~~~~~~~~~

Here is a sample job package that does a word count. It relies on a Pig
job to first upload the text file onto HDFS. One can also manually
upload a file and run the word count program alone.The source code is in
``azkaban-plugins/plugins/jobtype/src/azkaban/jobtype/examples/java/WordCount.java``

`Download
java-wc.zip <https://s3.amazonaws.com/azkaban2/azkaban2/samplejobs/java-wc.zip>`__
(Uploaded May 13, 2013)

.. raw:: html

   </div>

--------------

*****
Pig Type
*****


Pig type is for running Pig jobs. In the ``azkaban-plugins`` repo, we
have included Pig types from pig-0.9.2 to pig-0.11.0. It is up to the
admin to alias one of them as the ``pig`` type for Azkaban users.

Pig type is built on using hadoop tokens to talk to secure Hadoop
clusters. Therefore, individual Azkaban Pig jobs are restricted to run
within the token's lifetime, which is set by Hadoop admins. It is also
important that individual MR step inside a single Pig script doesn't
cancel the tokens upon its completion. Otherwise, all following steps
will fail on authentication with job tracker or name node.

Vanilla Pig types don't provide all udf jars. It is often up to the
admin who sets up Azkaban to provide a pre-configured Pig job type with
company specific udfs registered and name space imported, so that the
users don't need to provide all the jars and do the configurations in
their specific Pig job conf files.

How to Use
~~~~~~~~~~


The Pig job runs user Pig scripts. It is important to remember, however,
that running any Pig script might require a number of dependency
libraries that need to be placed on local Azkaban job classpath, or be
registered with Pig and carried remotely, or both. By using classpath
settings, as well as ``pig.additional.jars`` and ``udf.import.list``,
the admin can create a Pig job type that has very different default
behavior than the most basic "pig" type. Pig jobs talk to a secure
cluster via hadoop tokens. The admin should specify
``obtain.binary.token=true`` if the hadoop cluster security is turned
on. Before executing a job, Azkaban will obtain name node and job
tracker tokens for this job. These tokens will be written to a token
file, which will be picked up by user job process during its execution.
For Hadoop 1 (``HadoopSecurityManager_H_1_0``), after the job finishes,
Azkaban takes care of canceling these tokens from name node and job
tracker. In Hadoop 2 (``HadoopSecurityManager_H_2_0``), due to issues
with tokens being canceled prematurely, Azkaban does not cancel the
tokens.

Since Azkaban only obtains the tokens at the beginning of the job run,
and does not request new tokens or renew old tokens during the
execution, it is important that the job does not run longer than
configured token life. It is also important that individual MR step
inside a single Pig script doesn't cancel the tokens upon its
completion. Otherwise, all following steps will fail on authentication
with hadoop services. In Hadoop 2, you may need to set
``-Dmapreduce.job.complete.cancel.delegation.tokens=false`` to prevent
tokens from being canceled prematurely.

Here are the common configurations that make a Pig job for a *user*:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| type                              | The type name as set by the       |
|                                   | admin, e.g. ``pig``               |
+-----------------------------------+-----------------------------------+
| pig.script                        | The Pig script location. e.g.     |
|                                   | ``src/wordcountpig.pig``          |
+-----------------------------------+-----------------------------------+
| classpath                         | The resources that should be on   |
|                                   | the execution classpath,          |
|                                   | accessible to the local           |
|                                   | filesystem.                       |
+-----------------------------------+-----------------------------------+
| dependencies                      | The other jobs in the flow this   |
|                                   | job is dependent upon.            |
+-----------------------------------+-----------------------------------+
| user.to.proxy                     | The hadoop user this job should   |
|                                   | run under.                        |
+-----------------------------------+-----------------------------------+
| pig.home                          | The Pig installation directory.   |
|                                   | Can be used to override the       |
|                                   | default set by Azkaban.           |
+-----------------------------------+-----------------------------------+
| param.SOME_PARAM                  | Equivalent to Pig's ``-param``    |
+-----------------------------------+-----------------------------------+
| use.user.pig.jar                  | If true, will use the             |
|                                   | user-provided Pig jar to launch   |
|                                   | the job. If false, the Pig jar    |
|                                   | provided by Azkaban will be used. |
|                                   | Defaults to false.                |
+-----------------------------------+-----------------------------------+
| hadoop-inject.FOO                 | FOO is automatically added to the |
|                                   | Configuration of any Hadoop job   |
|                                   | launched.                         |
+-----------------------------------+-----------------------------------+

Here are what's needed and normally configured by the admin:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| hadoop.security.manager.class     | The class that handles talking to |
|                                   | hadoop clusters.                  |
+-----------------------------------+-----------------------------------+
| azkaban.should.proxy              | Whether Azkaban should proxy as   |
|                                   | individual user hadoop accounts.  |
+-----------------------------------+-----------------------------------+
| proxy.user                        | The Azkaban user configured with  |
|                                   | kerberos and hadoop, for secure   |
|                                   | clusters.                         |
+-----------------------------------+-----------------------------------+
| proxy.keytab.location             | The location of the keytab file   |
|                                   | with which Azkaban can            |
|                                   | authenticate with Kerberos for    |
|                                   | the specified proxy.user          |
+-----------------------------------+-----------------------------------+
| hadoop.home                       | The hadoop home where the jars    |
|                                   | and conf resources are installed. |
+-----------------------------------+-----------------------------------+
| jobtype.classpath                 | The items that every such job     |
|                                   | should have on its classpath.     |
+-----------------------------------+-----------------------------------+
| jobtype.class                     | Should be set to                  |
|                                   | ``azkaban.jobtype.HadoopJavaJob`` |
+-----------------------------------+-----------------------------------+
| obtain.binary.token               | Whether Azkaban should request    |
|                                   | tokens. Set this to true for      |
|                                   | secure clusters.                  |
+-----------------------------------+-----------------------------------+

Dumping MapReduce Counters: this is useful in the case where a Pig
script uses UDFs, which may add a few custom MapReduce counters

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| pig.dump.hadoopCounter            | Setting the value of this         |
|                                   | parameter to true will trigger    |
|                                   | the dumping of MapReduce counters |
|                                   | for each of the generated         |
|                                   | MapReduce job generated by the    |
|                                   | Pig script.                       |
+-----------------------------------+-----------------------------------+

Since Pig jobs are essentially Java programs, the configurations for
Java jobs could also be set.

Since Azkaban job types are named by their directory names, the admin
should also make those naming public and consistent. For example, while
there are multiple versions of Pig job types, the admin can link one of
them as ``pig`` for default Pig type. Experimental Pig versions can be
tested in parallel with a different name and can be promoted to default
Pig type if it is proven stable. In LinkedIn, we also provide Pig job
types that have a number of useful udf libraries, including datafu and
LinkedIn specific ones, pre-registered and imported, so that users in
most cases will only need Pig scripts in their Azkaban job packages.

.. raw:: html

   <div class="bs-callout bs-callout-info">

Sample Job Package
~~~~~~~~~~~~~~~~~~


Here is a sample job package that does word count. It assumes you have
hadoop installed and gets some dependency jars from ``$HADOOP_HOME``:

`Download
pig-wc.zip <https://s3.amazonaws.com/azkaban2/azkaban2/samplejobs/pig-wc.zip>`__
(Uploaded May 13, 2013)

.. raw:: html

   </div>

--------------

*****
Hive Type
*****

The ``hive`` type is for running Hive jobs. In the
`azkaban-plugins <https://github.com/azkaban/azkaban-plugins>`__ repo,
we have included hive type based on hive-0.8.1. It should work for
higher version Hive versions as well. It is up to the admin to alias one
of them as the ``hive`` type for Azkaban users.

The ``hive`` type is built using Hadoop tokens to talk to secure Hadoop
clusters. Therefore, individual Azkaban Hive jobs are restricted to run
within the token's lifetime, which is set by Hadoop admin. It is also
important that individual MR step inside a single Pig script doesn't
cancel the tokens upon its completion. Otherwise, all following steps
will fail on authentication with the JobTracker or NameNode.

How to Use
~~~~~~~~~~

The Hive job runs user Hive queries. The Hive job type talks to a secure
cluster via Hadoop tokens. The admin should specify
``obtain.binary.token=true`` if the Hadoop cluster security is turned
on. Before executing a job, Azkaban will obtain NameNode and JobTracker
tokens for this job. These tokens will be written to a token file, which
will be picked up by user job process during its execution. After the
job finishes, Azkaban takes care of canceling these tokens from NameNode
and JobTracker.

Since Azkaban only obtains the tokens at the beginning of the job run,
and does not request new tokens or renew old tokens during the
execution, it is important that the job does not run longer than
configured token life. It is also important that individual MR step
inside a single Pig script doesn't cancel the tokens upon its
completion. Otherwise, all following steps will fail on authentication
with Hadoop services.

Here are the common configurations that make a ``hive`` job for single
line Hive query:

+-----------------+--------------------------------------------------+
| Parameter       | Description                                      |
+=================+==================================================+
| type            | The type name as set by the admin, e.g. ``hive`` |
+-----------------+--------------------------------------------------+
| azk.hive.action | use ``execute.query``                            |
+-----------------+--------------------------------------------------+
| hive.query      | Used for single line hive query.                 |
+-----------------+--------------------------------------------------+
| user.to.proxy   | The hadoop user this job should run under.       |
+-----------------+--------------------------------------------------+

Specify these for a multi-line Hive query:

+-----------------+-------------------------------------------------------+
| Parameter       | Description                                           |
+=================+=======================================================+
| type            | The type name as set by the admin, e.g. ``hive``      |
+-----------------+-------------------------------------------------------+
| azk.hive.action | use ``execute.query``                                 |
+-----------------+-------------------------------------------------------+
| hive.query.01   | fill in the individual hive queries, starting from 01 |
+-----------------+-------------------------------------------------------+
| user.to.proxy   | The Hadoop user this job should run under.            |
+-----------------+-------------------------------------------------------+

Specify these for query from a file:

+-----------------+--------------------------------------------------+
| Parameter       | Description                                      |
+=================+==================================================+
| type            | The type name as set by the admin, e.g. ``hive`` |
+-----------------+--------------------------------------------------+
| azk.hive.action | use ``execute.query``                            |
+-----------------+--------------------------------------------------+
| hive.query.file | location of the query file                       |
+-----------------+--------------------------------------------------+
| user.to.proxy   | The Hadoop user this job should run under.       |
+-----------------+--------------------------------------------------+

Here are what's needed and normally configured by the admin. The
following properties go into private.properties:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| hadoop.security.manager.class     | The class that handles talking to |
|                                   | hadoop clusters.                  |
+-----------------------------------+-----------------------------------+
| azkaban.should.proxy              | Whether Azkaban should proxy as   |
|                                   | individual user hadoop accounts.  |
+-----------------------------------+-----------------------------------+
| proxy.user                        | The Azkaban user configured with  |
|                                   | kerberos and hadoop, for secure   |
|                                   | clusters.                         |
+-----------------------------------+-----------------------------------+
| proxy.keytab.location             | The location of the keytab file   |
|                                   | with which Azkaban can            |
|                                   | authenticate with Kerberos for    |
|                                   | the specified proxy.user          |
+-----------------------------------+-----------------------------------+
| hadoop.home                       | The hadoop home where the jars    |
|                                   | and conf resources are installed. |
+-----------------------------------+-----------------------------------+
| jobtype.classpath                 | The items that every such job     |
|                                   | should have on its classpath.     |
+-----------------------------------+-----------------------------------+
| jobtype.class                     | Should be set to                  |
|                                   | ``azkaban.jobtype.HadoopJavaJob`` |
+-----------------------------------+-----------------------------------+
| obtain.binary.token               | Whether Azkaban should request    |
|                                   | tokens. Set this to true for      |
|                                   | secure clusters.                  |
+-----------------------------------+-----------------------------------+
| hive.aux.jars.path                | Where to find auxiliary library   |
|                                   | jars                              |
+-----------------------------------+-----------------------------------+
| env.HADOOP_HOME                   | ``$HADOOP_HOME``                  |
+-----------------------------------+-----------------------------------+
| env.HIVE_HOME                     | ``$HIVE_HOME``                    |
+-----------------------------------+-----------------------------------+
| env.HIVE_AUX_JARS_PATH            | ``${hive.aux.jars.path}``         |
+-----------------------------------+-----------------------------------+
| hive.home                         | ``$HIVE_HOME``                    |
+-----------------------------------+-----------------------------------+
| hive.classpath.items              | Those that needs to be on hive    |
|                                   | classpath, include the conf       |
|                                   | directory                         |
+-----------------------------------+-----------------------------------+

These go into plugin.properties

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| job.class                         | ``azkaban.jobtype.hiveutils.azkab |
|                                   | an.HiveViaAzkaban``               |
+-----------------------------------+-----------------------------------+
| hive.aux.jars.path                | Where to find auxiliary library   |
|                                   | jars                              |
+-----------------------------------+-----------------------------------+
| env.HIVE_HOME                     | ``$HIVE_HOME``                    |
+-----------------------------------+-----------------------------------+
| env.HIVE_AUX_JARS_PATH            | ``${hive.aux.jars.path}``         |
+-----------------------------------+-----------------------------------+
| hive.home                         | ``$HIVE_HOME``                    |
+-----------------------------------+-----------------------------------+
| hive.jvm.args                     | ``-Dhive.querylog.location=.``    |
|                                   | ``-Dhive.exec.scratchdir=YOUR_HIV |
|                                   | E_SCRATCH_DIR``                   |
|                                   | ``-Dhive.aux.jars.path=${hive.aux |
|                                   | .jars.path}``                     |
+-----------------------------------+-----------------------------------+

Since hive jobs are essentially java programs, the configurations for
Java jobs could also be set.

.. raw:: html

   <div class="bs-callout bs-callout-info">

.. rubric:: Sample Job Package
   :name: sample-job-package-3

Here is a sample job package. It assumes you have hadoop installed and
gets some dependency jars from ``$HADOOP_HOME``. It also assumes you
have Hive installed and configured correctly, including setting up a
MySQL instance for Hive Metastore.

`Download
hive.zip <https://s3.amazonaws.com/azkaban2/azkaban2/samplejobs/hive.zip>`__
(Uploaded May 13, 2013)

.. raw:: html

   </div>

--------------

.. rubric:: New Hive Jobtype
   :name: new-hive-type

We've added a new Hive jobtype whose jobtype class is
``azkaban.jobtype.HadoopHiveJob``. The configurations have changed from
the old Hive jobtype.

Here are the configurations that a user can set:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| type                              | The type name as set by the       |
|                                   | admin, e.g. ``hive``              |
+-----------------------------------+-----------------------------------+
| hive.script                       | The relative path of your Hive    |
|                                   | script inside your Azkaban zip    |
+-----------------------------------+-----------------------------------+
| user.to.proxy                     | The hadoop user this job should   |
|                                   | run under.                        |
+-----------------------------------+-----------------------------------+
| hiveconf.FOO                      | FOO is automatically added as a   |
|                                   | hiveconf variable. You can        |
|                                   | reference it in your script using |
|                                   | ${hiveconf:FOO}. These variables  |
|                                   | also get added to the             |
|                                   | configuration of any launched     |
|                                   | Hadoop jobs.                      |
+-----------------------------------+-----------------------------------+
| hivevar.FOO                       | FOO is automatically added as a   |
|                                   | hivevar variable. You can         |
|                                   | reference it in your script using |
|                                   | ${hivevar:FOO}. These variables   |
|                                   | are NOT added to the              |
|                                   | configuration of launched Hadoop  |
|                                   | jobs.                             |
+-----------------------------------+-----------------------------------+
| hadoop-inject.FOO                 | FOO is automatically added to the |
|                                   | Configuration of any Hadoop job   |
|                                   | launched.                         |
+-----------------------------------+-----------------------------------+

Here are what's needed and normally configured by the admin. The
following properties go into private.properties (or into
../commonprivate.properties):

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| hadoop.security.manager.class     | The class that handles talking to |
|                                   | hadoop clusters.                  |
+-----------------------------------+-----------------------------------+
| azkaban.should.proxy              | Whether Azkaban should proxy as   |
|                                   | individual user hadoop accounts.  |
+-----------------------------------+-----------------------------------+
| proxy.user                        | The Azkaban user configured with  |
|                                   | kerberos and hadoop, for secure   |
|                                   | clusters.                         |
+-----------------------------------+-----------------------------------+
| proxy.keytab.location             | The location of the keytab file   |
|                                   | with which Azkaban can            |
|                                   | authenticate with Kerberos for    |
|                                   | the specified proxy.user          |
+-----------------------------------+-----------------------------------+
| hadoop.home                       | The hadoop home where the jars    |
|                                   | and conf resources are installed. |
+-----------------------------------+-----------------------------------+
| jobtype.classpath                 | The items that every such job     |
|                                   | should have on its classpath.     |
+-----------------------------------+-----------------------------------+
| jobtype.class                     | Should be set to                  |
|                                   | ``azkaban.jobtype.HadoopHiveJob`` |
+-----------------------------------+-----------------------------------+
| obtain.binary.token               | Whether Azkaban should request    |
|                                   | tokens. Set this to true for      |
|                                   | secure clusters.                  |
+-----------------------------------+-----------------------------------+
| obtain.hcat.token                 | Whether Azkaban should request    |
|                                   | HCatalog/Hive Metastore tokens.   |
|                                   | If true, the                      |
|                                   | HadoopSecurityManager will        |
|                                   | acquire an HCatalog token.        |
+-----------------------------------+-----------------------------------+
| hive.aux.jars.path                | Where to find auxiliary library   |
|                                   | jars                              |
+-----------------------------------+-----------------------------------+
| hive.home                         | ``$HIVE_HOME``                    |
+-----------------------------------+-----------------------------------+

These go into plugin.properties (or into ../common.properties):

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| hive.aux.jars.path                | Where to find auxiliary library   |
|                                   | jars                              |
+-----------------------------------+-----------------------------------+
| hive.home                         | ``$HIVE_HOME``                    |
+-----------------------------------+-----------------------------------+
| jobtype.jvm.args                  | ``-Dhive.querylog.location=.``    |
|                                   | ``-Dhive.exec.scratchdir=YOUR_HIV |
|                                   | E_SCRATCH_DIR``                   |
|                                   | ``-Dhive.aux.jars.path=${hive.aux |
|                                   | .jars.path}``                     |
+-----------------------------------+-----------------------------------+

Since hive jobs are essentially java programs, the configurations for
Java jobs can also be set.

--------------

*****
Common Configurations
*****


This section lists out the configurations that are common to all job
types

other_namenodes
~~~~~~~~~~~~~~~


This job property is useful for jobs that need to read data from or
write data to more than one Hadoop NameNode. By default Azkaban requests
a HDFS_DELEGATION_TOKEN on behalf of the job for the cluster that
Azkaban is configured to run on. When this property is present, Azkaban
will try request a HDFS_DELEGATION_TOKEN for each of the specified HDFS
NameNodes.

The value of this propety is in the form of comma separated list of
NameNode URLs.

For example: **other_namenodes=webhdfs://host1:50070,hdfs://host2:9000**

HTTP Job Callback
~~~~~~~~~~~~~~~~~


The purpose of this feature to allow Azkaban to notify external systems
via an HTTP upon the completion of a job. The new properties are in the
following format:

-  **job.notification.<status>.<sequence number>.url**
-  **job.notification.<status>.<sequence number>.method**
-  **job.notification.<status>.<sequence number>.body**
-  **job.notification.<status>.<sequence number>.headers**

Supported values for **status**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


-  **started**: when a job is started
-  **success**: when a job is completed successfully
-  **failure**: when a job failed
-  **completed**: when a job is either successfully completed or failed

Number of callback URLs
~~~~~~~~~~~~~~~~~~~~~~~


The maximum # of callback URLs per job is 3. So the <sequence number>
can go up from 1 to 3. If a gap is detected, only the ones before the
gap is used.

HTTP Method
~~~~~~~~~~~


The supported method are **GET** and **POST**. The default method is
**GET**

Headers
~~~~~~~


Each job callback URL can optional specify headers in the following
format

**job.notification.<status>.<sequence
number>.headers**\ =<name>:<value>\r\n<name>:<value>
The delimiter for each header is '\r\n' and delimiter between header
name and value is ':'

The headers are applicable for both GET and POST job callback URLs.

Job Context Information
~~~~~~~~~~~~~~~~~~~~~~~


It is often desirable to include some dynamic context information about
the job in the URL or POST request body, such as status, job name, flow
name, execution id and project name. If the URL or POST request body
contains any of the following tokens, they will be replaced with the
actual values by Azkabn before making the HTTP callback is made. The
value of each token will be HTTP encoded.

-  **?{server}** - Azkaban host name and port
-  **?{project}**
-  **?{flow}**
-  **?{executionId}**
-  **?{job}**
-  **?{status}** - possible values are started, failed, succeeded

The value of these tokens will be HTTP encoded if they are on the URL,
but will not be encoded when they are in the HTTP body.

Examples
~~~~~~~~


GET HTTP Method

-  job.notification.started.1.url=http://abc.com/api/v2/message?text=wow!!&job=?{job}&status=?{status}
-  job.notification.completed.1.url=http://abc.com/api/v2/message?text=wow!!&job=?{job}&status=?{status}
-  job.notification.completed.2.url=http://abc.com/api/v2/message?text=yeah!!

POST HTTP Method

-  job.notification.started.1.url=http://abc.com/api/v1/resource
-  job.notification.started.1.method=POST
-  job.notification.started.1.body={"type":"workflow",
   "source":"Azkaban",
   "content":"{server}:?{project}:?{flow}:?{executionId}:?{job}:?{status}"}
-  job.notification.started.1.headers=Content-type:application/json

--------------

*****
VoldemortBuildandPush Type
*****

Pushing data from hadoop to voldemort store used to be entirely in java.
This created lots of problems, mostly due to users having to keep track
of jars and dependencies and keep them up-to-date. We created the
``VoldemortBuildandPush`` job type to address this problem. Jars and
dependencies are now managed by admins; absolutely no jars or java code
are required from users.

How to Use
~~~~~~~~~~


This is essentially a hadoopJava job, with all jars controlled by the
admins. User only need to provide a .job file for the job and specify
all the parameters. The following needs to be specified:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| type                              | The type name as set by the       |
|                                   | admin, e.g.                       |
|                                   | ``VoldemortBuildandPush``         |
+-----------------------------------+-----------------------------------+
| push.store.name                   | The voldemort push store name     |
+-----------------------------------+-----------------------------------+
| push.store.owners                 | The push store owners             |
+-----------------------------------+-----------------------------------+
| push.store.description            | Push store description            |
+-----------------------------------+-----------------------------------+
| build.input.path                  | Build input path on hdfs          |
+-----------------------------------+-----------------------------------+
| build.output.dir                  | Build output path on hdfs         |
+-----------------------------------+-----------------------------------+
| build.replication.factor          | replication factor number         |
+-----------------------------------+-----------------------------------+
| user.to.proxy                     | The hadoop user this job should   |
|                                   | run under.                        |
+-----------------------------------+-----------------------------------+
| build.type.avro                   | if build and push avro data,      |
|                                   | true, otherwise, false            |
+-----------------------------------+-----------------------------------+
| avro.key.field                    | if using Avro data, key field     |
+-----------------------------------+-----------------------------------+
| avro.value.field                  | if using Avro data, value field   |
+-----------------------------------+-----------------------------------+

Here are what's needed and normally configured by the admn (always put
common properties in ``commonprivate.properties`` and
``common.properties`` for all job types).

These go into ``private.properties``:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| hadoop.security.manager.class     | The class that handles talking to |
|                                   | hadoop clusters.                  |
+-----------------------------------+-----------------------------------+
| azkaban.should.proxy              | Whether Azkaban should proxy as   |
|                                   | individual user hadoop accounts.  |
+-----------------------------------+-----------------------------------+
| proxy.user                        | The Azkaban user configured with  |
|                                   | kerberos and hadoop, for secure   |
|                                   | clusters.                         |
+-----------------------------------+-----------------------------------+
| proxy.keytab.location             | The location of the keytab file   |
|                                   | with which Azkaban can            |
|                                   | authenticate with Kerberos for    |
|                                   | the specified ``proxy.user``      |
+-----------------------------------+-----------------------------------+
| hadoop.home                       | The hadoop home where the jars    |
|                                   | and conf resources are installed. |
+-----------------------------------+-----------------------------------+
| jobtype.classpath                 | The items that every such job     |
|                                   | should have on its classpath.     |
+-----------------------------------+-----------------------------------+
| jobtype.class                     | Should be set to                  |
|                                   | ``azkaban.jobtype.HadoopJavaJob`` |
+-----------------------------------+-----------------------------------+
| obtain.binary.token               | Whether Azkaban should request    |
|                                   | tokens. Set this to true for      |
|                                   | secure clusters.                  |
+-----------------------------------+-----------------------------------+
| azkaban.no.user.classpath         | Set to true such that Azkaban     |
|                                   | doesn't pick up user supplied     |
|                                   | jars.                             |
+-----------------------------------+-----------------------------------+

These go into ``plugin.properties``:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| job.class                         | ``voldemort.store.readonly.mr.azk |
|                                   | aban.VoldemortBuildAndPushJob``   |
+-----------------------------------+-----------------------------------+
| voldemort.fetcher.protocol        | ``webhdfs``                       |
+-----------------------------------+-----------------------------------+
| hdfs.default.classpath.dir        | HDFS location for distributed     |
|                                   | cache                             |
+-----------------------------------+-----------------------------------+
| hdfs.default.classpath.dir.enable | set to true if using distributed  |
|                                   | cache to ship dependency jars     |
+-----------------------------------+-----------------------------------+

.. raw:: html

   <div class="bs-callout bs-callout-info">

For more information
~~~~~~~~~~~~~~~~~~~~


Please refer to `Voldemort project
site <http://project-voldemort.com/voldemort>`__ for more info.

.. raw:: html

   </div>

--------------

*****
Create Your Own Jobtypes
*****


With plugin design of Azkaban job types, it is possible to extend
Azkaban for various system environments. You should be able to execute
any job under the same Azkaban work flow management and scheduling.

Creating new job types is often times very easy. Here are several ways
one can do it:

New Types with only Configuration Changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


One doesn't always need to write java code to create job types for end
users. Often times, configuration changes of existing job types would
create significantly different behavior to the end users. For example,
in LinkedIn, apart from the *pig* types, we also have *pigLi* types that
come with all the useful library jars pre-registered and imported. This
way, normal users only need to provide their pig scripts, and the their
own udf jars to Azkaban. The pig job should run as if it is run on the
gateway machine from pig grunt. In comparison, if users are required to
use the basic *pig* job types, they will need to package all the
necessary jars in the Azkaban job package, and do all the register and
import by themselves, which often poses some learning curve for new
pig/Azkaban users.

The same practice applies to most other job types. Admins should create
or tailor job types to their specific company needs or clusters.

New Types Using Existing Job Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


If one needs to create a different job type, a good starting point is to
see if this can be done by using an existing job type. In hadoop land,
this most often means the hadoopJava type. Essentially all hadoop jobs,
from the most basic mapreduce job, to pig, hive, crunch, etc, are java
programs that submit jobs to hadoop clusters. It is usually straight
forward to create a job type that takes user input and runs a hadoopJava
job.

For example, one can take a look at the VoldemortBuildandPush job type.
It will take in user input such as which cluster to push to, voldemort
store name, etc, and runs hadoopJava job that does the work. For end
users though, this is a VoldemortBuildandPush job type with which they
only need to fill out the ``.job`` file to push data from hadoop to
voldemort stores.

The same applies to the hive type.

New Types by Extending Existing Ones
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For the most flexibility, one can always build new types by extending
the existing ones. Azkaban uses reflection to load job types that
implements the ``job`` interface, and tries to construct a sample object
upon loading for basic testing. When executing a real job, Azkaban calls
the ``run`` method to run the job, and ``cancel`` method to cancel it.

For new hadoop job types, it is important to use the correct
``hadoopsecuritymanager`` class, which is also included in
``azkaban-plugins`` repo. This class handles talking to the hadoop
cluster, and if needed, requests tokens for job execution or for name
node communication.

For better security, tokens should be requested in Azkaban main process
and be written to a file. Before executing user code, the job type
should implement a wrapper that picks up the token file, set it in the
``Configuration`` or ``JobConf`` object. Please refer to
``HadoopJavaJob`` and ``HadoopPigJob`` to see example usage.

--------------

*****
System Statistics
*****


Azkaban server maintains certain system statistics and they be seen
http:<host>:<port>/stats

To enable this feature, add the following property
"executor.metric.reports=true" to azkaban.properties

Property "executor.metric.milisecinterval.default" controls the interval
at which the metrics are collected at

Statistic Types
~~~~~~~~~~~~~~~


+----------------------+------------------------------+
| Metric Name          | Description                  |
+======================+==============================+
| NumFailedFlowMetric  | Number of failed flows       |
+----------------------+------------------------------+
| NumRunningFlowMetric | Number of flows in the queue |
+----------------------+------------------------------+
| NumQueuedFlowMetric  | Number of flows in the queue |
+----------------------+------------------------------+
| NumRunningJobMetric  | Number of running jobs       |
+----------------------+------------------------------+
| NumFailedJobMetric   | Number of failed jobs        |
+----------------------+------------------------------+

To change the statistic collection at run time, the following options
are available

-  To change the time interval at which the specific type of statistics
   are collected -
   /stats?action=changeMetricInterval&metricName=NumRunningJobMetric&interval=60000
-  To change the duration at which the statistics are maintained
   -/stats?action=changeCleaningInterval&interval=604800000
-  To change the number of data points to display -
   /stats?action=changeEmitterPoints&numInstances=50
-  To enable the statistic collection - /stats?action=enableMetrics
-  To disable the statistic collection - /stats?action=disableMetrics

--------------

*****
Reload Jobtypes
*****

When you want to make changes to your jobtype configurations or
add/remove jobtypes, you can do so without restarting the executor
server. You can reload all jobtype plugins as follows:

.. code-block:: guess

   curl http://localhost:EXEC_SERVER_PORT/executor?action=reloadJobTypePlugins


