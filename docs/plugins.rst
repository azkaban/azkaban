Plugins
========
..
	TODO:Fix download page

Azkaban is designed to be modular. We are able to plug in code to add
viewer pages or execute jobs in a customizable manner. These pages will
describe the azkaban-plugins that can be downloaded from `the download
page <%7B%7B%20site.home%20%7D%7D/downloads.html>`__ and how to extend
Azkaban by creating your own plugins or extending an existing one.

.. _hadoopsecuritymanager:

HadoopSecurityManager
---------------------------

The most common adoption of Azkaban has been in the big data platforms
such as Hadoop, etc. Azkaban's jobtype plugin system allows most
flexible support to such systems.

Azkaban is able to support all Hadoop versions, with support for Hadoop
security features; Azkaban is able to support various ecosystem
components with all different versions, such as different versions of
pig, hive, on the same instance.

A common pattern to achieve this is by using the
``HadoopSecurityManager`` class, which handles talking to a Hadoop
cluster and take care of Hadoop security, in a secure way.

Hadoop Security with Kerberos, Hadoop Tokens
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When Hadoop is used in enterprise production environment, it is
advisable to have its security feature turned on, to protect your data
and guard against mistakes.

**Kerberos Authentication**

The most common authentication provided by Apache Hadoop is via
Kerberos, which requires a KDC to authenticate users and services.

A user can authenticate with KDC via username/password or use a keytab.
KDC distributes a tgt to authenticated users. Hadoop services, such as
name node and job tracker, can use this tgt to verify this is
authenticated user.

**Hadoop Tokens**

Once a user is authenticated with Hadoop services, Hadoop will issue
tokens to the user so that its internal services won't flood KDC. For a
description of tokens, see
`here <http://hortonworks.com/blog/the-role-of-delegation-tokens-in-apache-hadoop-security/>`__.

**Hadoop SecurityManager**

For human users, one authenticate with KDC with a kinit command. But for
scheduler such as Azkaban that runs jobs on behalf as other users, it
needs to acquire tokens that will be used by the users. Specific Azkaban
job types should handle this, with the use of ``HadoopSecurityManager``
class.

For instance, when Azkaban loads the pig job type, it will initiate a
HadoopSecurityManager that is authenticated with the desired KDC and
Hadoop Cluster. The pig job type conf should specify which tokens are
needed to talk to different services. At minimum it needs tokens from
name node and job tracker. When a pig job starts, it will go to the
HadoopSecurityManager to acquire all those tokens. When the user process
finishes, the pig job type calls HadoopSecurityManager again to cancel
all those tokens.

Settings Common to All Hadoop Clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a user program wants to talk to a Hadoop cluster, it needs to know
where are the name node and job tracker. It also needs to know how to
authenticate with them. These information are all in the Hadoop config
files that are normally in ``$HADOOP_HOME/conf``. For this reason, this
conf directory as well as the hadoop-core jar need to be on azkaban
executor server classpath.

If you are using Hive that uses HCat as its metastore, you also need
relevant hive jars and hive conf on the classpath as well.

**Native Library**

Most likely your Hadoop platform depends on some native library, this
should be specified in java.library.path in azkaban executor server.

**temp dir**

Besides those, many tools on Hadoop, such as Pig/Hive/Crunch write files
into temporary directory. By default, they all go to ``/tmp``. This
could cause operations issue when a lot of jobs run concurrently.
Because of this, you may want to change this by setting
``java.io.tmp.dir`` to a different directory.

Settings To Talk to UNSECURE Hadoop Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are just starting out with Hadoop, chances are you don't have
kerberos authentication for your Hadoop. Depending on whether you want
to run everything as azkaban user (or whatever user started the azkaban
executor server), you can do the following settings:

-  If you started the executor server with user named azkaban, and you
   want to run all the jobs as azkaban on Hadoop, just set
   ``azkaban.should.proxy=false`` and ``obtain.binary.token=false``
-  If you started the executor server with user named azkaban, but you
   want to run Hadoop jobs as their individual users, you need to set
   ``azkaban.should.proxy=true`` and ``obtain.binary.token=false``

Settings To Talk to SECURE Hadoop Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For secure Hadoop clusters, Azkaban needs its own kerberos keytab to
authenticate with KDC. Azkaban job types should acquire necessary Hadoop
tokens before user job process starts, and should cancel the tokens
after user job finishes.

All job type specific settings should go to their respective plugin conf
files. Some of the common settings can go to commonprivate.properties
and common.properties.

For instance, Hadoop job types usually require name node tokens and job
tracker tokens. These can go to commonpriate.properties.

**Azkaban as proxy user**

The following settings are needed for HadoopSecurityManager to
authenticate with KDC:

::

   proxy.user=YOUR_AZKABAN_KERBEROS_PRINCIPAL

This principal should also be set in core-site.xml in Hadoop conf with
corresponding permissions.

::

   proxy.keytab.location=KEYTAB_LOCATION

One should verify if the proxy user and keytab works with the specified
KDC.

**Obtaining tokens for user jobs**

Here are what's common for most Hadoop jobs

::

   hadoop.security.manager.class=azkaban.security.HadoopSecurityManager_H_1_0

This implementation should work with Hadoop 1.x

::

   azkaban.should.proxy=true
   obtain.binary.token=true
   obtain.namenode.token=true
   obtain.jobtracker.token=true

Additionally, if your job needs to talk to HCat, for example if you have
Hive installed with uses kerbrosed HCat, or your pig job needs to talk
to HCat, you will need to set for those Hive job types

::

   obtain.hcat.token=true

This makes HadoopSecurityManager acquire a HCat token as well.

Making a New Job Type on Secure Hadoop Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are making a new job type that will talk to Hadoop Cluster, you
can use the HadoopSecurityManager to take care of security.

For unsecure Hadoop cluster, there is nothing special that is needed.

For secure Hadoop clusters, there are two ways inlcuded in the
hadoopsecuritymanager package:

-  give the key tab information to user job process. The
   hadoopsecuritymanager static method takes care of login from that
   common keytab and proxy to the user. This is convenient for
   prototyping as there will be a real tgt granted to the user job. The
   con side is that the user could potentially use the keytab to login
   and proxy as someone else, which presents a security hole.
-  obtain Hadoop tokens prior to user job process start. The job wrapper
   will pick up these binary tokens inside user job process. The tokens
   should be explicitly cancelled after user job finishes.

By paring properly configured hadoopsecuritymanager with basic job types
such as hadoopJava, pig, hive, one can make these job types work with
different versions of Hadoop with various security settings.

Included in the azkaban-plugins is the hadoopsecuritymanager for
Hadoop-1.x versions. It is not compatible with Hadoop-0.20 and prior
versions as Hadoop UGI is not backwards compatible. However, it should
not be difficult to implement one that works with them. Going forward,
Hadoop UGI is mostly backwards compatible and one only needs to
recompile hadoopsecuritymanager package with newer versions of Hadoop.

.. _hdfs-browser:

Azkaban HDFS Browser
--------------------

The Azkaban HDFS Browser is a plugin that allows you to view the HDFS
FileSystem and decode several file types. It was originally created at
LinkedIn to view Avro files, Linkedin's BinaryJson format and text
files. As this plugin matures further, we may add decoding of different
file types in the future.

.. image:: figures/hdfsbrowser.png

Setup
~~~~~
..
	TODO:Fix download page
	
Download the HDFS plugin from `the download
page <%7B%7B%20site.home%20%7D%7D/downloads.html>`__ and extract it into
the web server's plugin's directory. This is often
``azkaban_web_server_dir/plugins/viewer/``.

**Users**

By default, Azkaban HDFS browser does a do-as to impersonate the
logged-in user. Often times, data is created and handled by a headless
account. To view these files, if user proxy is turned on, then the user
can switch to the headless account as long as its validated by the
UserManager.

**Settings**

These are properties to configure the HDFS Browser on the
AzkabanWebServer. They can be set in
``azkaban_web_server_dir/plugins/viewer/hdfs/conf/plugin.properties``.

+-----------------------+-----------------------+-----------------------+
| Parameter             | Description           | Default               |
+=======================+=======================+=======================+
| viewer.name           | The name of this      | HDFS                  |
|                       | viewer plugin         |                       |
+-----------------------+-----------------------+-----------------------+
| viewer.path           | The path to this      | hdfs                  |
|                       | viewer plugin inside  |                       |
|                       | viewer directory.     |                       |
+-----------------------+-----------------------+-----------------------+
| viewer.order          | The order of this     | 1                     |
|                       | viewer plugin amongst |                       |
|                       | all viewer plugins.   |                       |
+-----------------------+-----------------------+-----------------------+
| viewer.hidden         | Whether this plugin   | false                 |
|                       | should show up on the |                       |
|                       | web UI.               |                       |
+-----------------------+-----------------------+-----------------------+
| viewer.external.class | Extra jars this       | extlib/\*             |
| path                  | viewer plugin should  |                       |
|                       | load upon init.       |                       |
+-----------------------+-----------------------+-----------------------+
| viewer.servlet.class  | The main servelet     |                       |
|                       | class for this viewer |                       |
|                       | plugin. Use           |                       |
|                       | ``azkaban.viewer.hdfs |                       |
|                       | .HdfsBrowserServlet`` |                       |
|                       | for hdfs browser      |                       |
+-----------------------+-----------------------+-----------------------+
| hadoop.security.manag | The class that        |                       |
| er.class              | handles talking to    |                       |
|                       | hadoop clusters. Use  |                       |
|                       | ``azkaban.security.Ha |                       |
|                       | doopSecurityManager_H |                       |
|                       | _1_0``                |                       |
|                       | for hadoop 1.x        |                       |
+-----------------------+-----------------------+-----------------------+
| azkaban.should.proxy  | Whether Azkaban       | false                 |
|                       | should proxy as       |                       |
|                       | individual user       |                       |
|                       | hadoop accounts on a  |                       |
|                       | secure cluster,       |                       |
|                       | defaults to false     |                       |
+-----------------------+-----------------------+-----------------------+
| proxy.user            | The Azkaban user      |                       |
|                       | configured with       |                       |
|                       | kerberos and hadoop.  |                       |
|                       | Similar to how oozie  |                       |
|                       | should be configured, |                       |
|                       | for secure hadoop     |                       |
|                       | installations         |                       |
+-----------------------+-----------------------+-----------------------+
| proxy.keytab.location | The location of the   |                       |
|                       | keytab file with      |                       |
|                       | which Azkaban can     |                       |
|                       | authenticate with     |                       |
|                       | Kerberos for the      |                       |
|                       | specified proxy.user  |                       |
+-----------------------+-----------------------+-----------------------+
| allow.group.proxy     | Whether to allow      | false                 |
|                       | users in the same     |                       |
|                       | headless user group   |                       |
|                       | to view hdfs          |                       |
|                       | filesystem as that    |                       |
|                       | headless user         |                       |
+-----------------------+-----------------------+-----------------------+

.. _jobtype-plugins:

JobType Plugins
---------------

Azkaban Jobtype Plugins Configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These are properties to configure the jobtype plugins that are installed
with the AzkabanExecutorServer. Note that Azkaban uses the directory
structure to infer global settings versus individual jobtype specific
settings. Sub-directory names also determine the job type name for
running Azkaban instances.

**Introduction**


Jobtype plugins determine how individual jobs are actually run locally
or on a remote cluster. It gives great benefits: one can add or change
any job type without touching Azkaban core code; one can easily extend
Azkaban to run on different hadoop versions or distributions; one can
keep old versions around while adding new versions of the same types.
However, it is really up to the admin who manages these plugins to make
sure they are installed and configured correctly.

Upon AzkabanExecutorServer start up, Azkaban will try to load all the
job type plugins it can find. Azkaban will do very simply tests and drop
the bad ones. One should always try to run some test jobs to make sure
the job types really work as expected.

**Global Properties**

One can pass global settings to all job types, including cluster
dependent settings that will be used by all job types. These settings
can also be specified in each job type's own settings as well.

**Private settings**

One can pass global settings that are needed by job types but should not
be accessible by user code in ``commonprivate.properties``. For example,
the following settings are often needed for a hadoop cluster:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| hadoop.security.manager.class     | The hadoopsecuritymanager that    |
|                                   | handles talking to a hadoop       |
|                                   | cluseter. Use                     |
|                                   | ``azkaban.security.HadoopSecurity |
|                                   | Manager_H_1_0``                   |
|                                   | for 1.x versions                  |
+-----------------------------------+-----------------------------------+
| azkaban.should.proxy              | Whether Azkaban should proxy as   |
|                                   | individual user hadoop accounts,  |
|                                   | or run as the Azkaban user        |
|                                   | itself, defaults to ``true``      |
+-----------------------------------+-----------------------------------+
| proxy.user                        | The Azkaban user configured with  |
|                                   | kerberos and hadoop. Similar to   |
|                                   | how oozie should be configured,   |
|                                   | for secure hadoop installations   |
+-----------------------------------+-----------------------------------+
| proxy.keytab.location             | The location of the keytab file   |
|                                   | with which Azkaban can            |
|                                   | authenticate with Kerberos for    |
|                                   | the specified proxy.user          |
+-----------------------------------+-----------------------------------+
| jobtype.global.classpath          | The jars or xml resources every   |
|                                   | job type should have on their     |
|                                   | classpath. (e.g.                  |
|                                   | ``${hadoop.home}/hadoop-core-1.0. |
|                                   | 4.jar,${hadoop.home}/conf``)      |
+-----------------------------------+-----------------------------------+
| jobtype.global.jvm.args           | The jvm args that every job type  |
|                                   | should have to jvm.               |
+-----------------------------------+-----------------------------------+
| hadoop.home                       | The ``$HADOOP_HOME`` setting.     |
+-----------------------------------+-----------------------------------+

**Public settings**

One can pass global settings that are needed by job types and can be
visible by user code, in ``common.properties``. For example,
``hadoop.home`` should normally be passed along to user programs.

**Settings for individual job types**

In most cases, there is no extra settings needed for job types to work,
other than variables like ``hadoop.home``, ``pig.home``, ``hive.home``,
etc. However, it is also where most of the customizations come from. For
example, one can configure a two Java job types with the same jar
resources but with different hadoop configurations, thereby submitting
pig jobs to different clusters. One can also configure pig job with
pre-registered jars and namespace imports for specific organizations.
Also to be noted: in the list of common job type plugins, we have
included different pig versions. The admin needs to make a soft link to
one of them, such as

::

   $ ln -s pig-0.10.1 pig

so that the users can use a default "pig" type.
