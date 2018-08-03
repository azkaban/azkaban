.. _GetStartedHead:


Getting Started
==================================

After version 3.0, we provide two modes: the stand alone "solo-server" mode and distributed multiple-executor mode. The following describes the differences between the two modes.

In solo server mode, the DB is embedded H2 and both web server and executor server run in the same process. This should be useful if one just wants to try things out. It can also be used on small scale use cases.

The multiple executor mode is for most serious production environment. Its DB should be backed by MySQL instances with master-slave set up. The web server and executor servers should ideally run in different hosts so that upgrading and maintenance shouldn't affect users. This multiple host setup brings in robust and scalable aspect to Azkaban.

- Set up the database
- Configure database to use multiple executors
- Download and install the Executor Server for each executor configured in database
- Install Azkaban Plugins
- Install the Web Server

Below are instructions on how to set Azkaban up.

*****
Building from Source
*****

Azkaban builds use Gradle (downloads automatically when run using gradlew which is the Gradle wrapper) and requires Java 8 or higher.

The following commands run on *nix platforms like Linux, OS X.
::
  # Build Azkaban
  ./gradlew build

  # Clean the build
  ./gradlew clean

  # Build and install distributions
  ./gradlew installDist

  # Run tests
  ./gradlew test

  # Build without running tests
  ./gradlew build -x test

These are all standard Gradle commands. Please look at Gradle documentation for more info.

Gradle creates .tar.gz files inside project directories. eg. ./azkaban-solo-server/build/distributions/azkaban-solo-server-0.1.0-SNAPSHOT.tar.gz. Untar using tar -xvzf path/to/azkaban-*.tar.gz.


*****
Getting started with the Solo Server
*****
The solo server is a standalone instance of Azkaban and the simplest to get started with. The solo server has the following advantages.

- **Easy to install** - No MySQL instance is needed. It packages H2 as its main persistence storage.
- **Easy to start up** - Both web server and executor server run in the same process.
- **Full featured** - It packages all Azkaban features. You can use it in normal ways and install plugins for it.


Installing the Solo Server
########

Follow these steps to get started:

1. Clone the repo:
::
  git clone https://github.com/azkaban/azkaban.git
2. Build Azkaban and create an installation package:
::
  cd azkaban; ./gradlew build installDist
3. Start the solo server:
::
  cd azkaban-solo-server/build/install/azkaban-solo-server; bin/azkaban-solo-start.sh
Azkaban solo server should be all set, by listening to ``8081`` port at default to accept incoming network request. So, open a web browser and check out ``http://localhost:8081/``

4. Stop server:
::
  bin/azkaban-solo-shutdown.sh


The solo-server installation should contain the following directories.

+----------+---------------------------------------------------------+
| Folder   | Description                                             |
+==========+=========================================================+
| bin      | The scripts to start/stop Azkaban solo server           |
|          |                                                         |
+----------+---------------------------------------------------------+
| conf     | The configuration files for Azkaban solo server         |
|          |                                                         |
+----------+---------------------------------------------------------+
| lib      | The jar dependencies for Azkaban                        |
|          |                                                         |
+----------+---------------------------------------------------------+
| extlib   | Additional jars that are added to extlib will be added  |
|          | to Azkaban's classpath                                  |
+----------+---------------------------------------------------------+
| plugins  | the directory where plugins can be installed            |
|          |                                                         |
+----------+---------------------------------------------------------+
| web      | The web (css, javascript, image) files for Azkaban web  |
|          | server                                                  |
+----------+---------------------------------------------------------+


Inside the ``conf`` directory, there should be three files:

- ``azkaban.private.properties`` - Used by Azkaban to store secrets like Mysql password
- ``azkaban.properties`` - Used by Azkaban for runtime parameters
- ``global.properties`` - Global static properties that are passed as shared properties to every workflow and job.
- ``azkaban-users.xml`` - Used to add users and roles for authentication. This file is not used if the XmLUserManager is not set up to use it.

The ``The azkaban.properties`` file is the main configuration file.


Configuring HTTPS server (*Optional*)
########

Azkaban solo server by default doesn't use SSL. But you could set it up the same way in a stand alone web server. Here is how:

Azkaban web server supports SSL socket connectors, which means a keystore will have to be available. You can follow the steps to generate a valid jetty keystore provided at `here <https://wiki.eclipse.org/Jetty/Howto/Configure_SSL>`_. Once a keystore file has been created, Azkaban must be given its location and password. Within ``azkaban.properties`` or ``azkaban.private.properties`` (recommended), the following properties should be overridden.
::
  jetty.keystore=keystore
  jetty.password=password
  jetty.keypassword=password
  jetty.truststore=keystore
  jetty.trustpassword=password

And configure ssl port in `azkaban.properties`:
::
  jetty.ssl.port=8443


*****
Getting started with the Multi Executor Server
*****

Databasea setup
########

We suggest users to opt for **Mysql** as Azkaban database, because we build up a few Mysql connection enhancements to facilitate AZ set up, and strengthen service reliability:


- Install Mysql

  Installation of MySQL DB won't be covered by these instructions, but you can access the instructions on `MySQL Documentation Site <https://dev.mysql.com/doc/>`_.

- Set up Mysql

   a. create database for Azkaban.::

         # Example database creation command, although the db name doesn't need to be 'azkaban'
         mysql> CREATE DATABASE azkaban;

   b. create a mysql user for Azkaban. For example,::

         # Example database creation command. The user name doesn't need to be 'azkaban'
         mysql> CREATE USER 'username'@'%' IDENTIFIED BY 'password';
         # give the user INSERT, SELECT, UPDATE, DELETE permission on all tables in the Azkaban db.
         mysql> GRANT SELECT,INSERT,UPDATE,DELETE ON azkaban.* to '<username>'@'%' WITH GRANT OPTION;

   c. Mysql Packet Size may need to be re-configured. MySQL may have, by default, a ridiculously low allowable packet size. To increase it, you'll need to have the property max_allowed_packet set to a higher number, say 1024M.
      To configure this in linux, open /etc/my.cnf. Somewhere after mysqld, add the following::

         [mysqld]
         ...
         max_allowed_packet=1024M

      To restart MySQL, you can run::

         $ sudo /sbin/service mysqld restart


- Create the Azkaban Tables

  Run individual table creation scripts from `latest table statements <https://github.com/azkaban/azkaban/tree/master/azkaban-db/src/main/sql>`_ on the MySQL instance to create your tables.

  Alternatively, run create-all-sql-<version>.sql generated by build process. The location is the file is at ``/Users/latang/LNKDRepos/azkaban/azkaban-db/build/distributions/azkaban-db-<version>``, after you build `azkaban-db` module by ::

    cd azkaban-db; ../gradlew build installDist

Installing Azkaban Executor Server
########

Azkaban Executor Server handles the actual execution of the workflow and jobs. You can build the latest version from the master branch. See here for instructions on `Building from Source`_.

Extract the package (executor distribution tar.gz from build folder) into a directory after gradle build. There should be the following directories.

+----------+---------------------------------------------------------+
| Folder   | Description                                             |
+==========+=========================================================+
| bin      | The scripts to start/stop Azkaban solo server           |
|          |                                                         |
+----------+---------------------------------------------------------+
| conf     | The configuration files for Azkaban solo server         |
|          |                                                         |
+----------+---------------------------------------------------------+
| lib      | The jar dependencies for Azkaban                        |
|          |                                                         |
+----------+---------------------------------------------------------+
| extlib   | Additional jars that are added to extlib will be added  |
|          | to Azkaban's classpath                                  |
+----------+---------------------------------------------------------+
| plugins  | the directory where plugins can be installed            |
|          |                                                         |
+----------+---------------------------------------------------------+

For quick start, we may directly use the Installation directory `azkaban/azkaban-exec-server/build/install/azkaban-exec-server` generated by gradle. we only need to change mysql username and password inside ``azkaban.properties``::

  # Mysql Configs
  mysql.user=<username>
  mysql.password=<password>

Then run::

  cd azkaban-solo-server/build/install/azkaban-exec-server
  ./bin/start-exec.sh

After that, remember to activate the executor by calling::

  cd azkaban-exec-server/build/install/azkaban-exec-server
  curl -G "localhost:$(<./executor.port)/executor?action=activate" && echo

Then, one executor is ready for use. Users can set up multiple executors by distributing and deploying multiple executor installation distributions.


Installing Azkaban Web Server
########

Azkaban Web Server handles project management, authentication, scheduling and trigger of executions. You can build the latest version from the master branch. See here for instructions on `Building from Source`_.

Extract the package (executor distribution tar.gz from build folder) into a directory after gradle build. There should be the following directories.

+----------+---------------------------------------------------------+
| Folder   | Description                                             |
+==========+=========================================================+
| bin      | The scripts to start/stop Azkaban solo server           |
|          |                                                         |
+----------+---------------------------------------------------------+
| conf     | The configuration files for Azkaban solo server         |
|          |                                                         |
+----------+---------------------------------------------------------+
| lib      | The jar dependencies for Azkaban                        |
|          |                                                         |
+----------+---------------------------------------------------------+
| web      | The web (css, javascript, image) files for Azkaban web  |
|          | server                                                  |
+----------+---------------------------------------------------------+


For quick start, we may directly use the Installation directory `azkaban/azkaban-web-server/build/install/azkaban-web-server` generated by gradle. we only need to change mysql username and password inside ``azkaban.properties``::

  # Mysql Configs
  mysql.user=<username>
  mysql.password=<password>

Then run ::

  cd azkaban-web-server/build/install/azkaban-web-server
  ./bin/start-web.sh

Then, a multi-executor Azkaban instance is ready for use. Open a web browser and check out ``http://localhost:8081/``
You are all set to login to Azkaban UI.

*****
Set up Azkaban Plugins
*****

Azkaban is designed to make non-core functionalities plugin-based, so
that

#. they can be selectively installed/upgraded in different environments
   without changing the core Azkaban, and
#. it makes Azkaban very easy to be extended for different systems.

Right now, Azkaban allows for a number of different plugins. On web
server side, there are

-  viewer plugins that enable custom web pages to add features to
   Azkaban. Some of the known implementations include HDFS filesystem
   viewer, and Reportal.
-  trigger plugins that enable custom triggering methods.
-  user manager plugin that enables custom user authentication methods.
   For instance, in LinkedIn we have LDAP based user authentication.
-  alerter plugins that enable different alerting methods to users, in
   addition to email based alerting.

On executor server side

-  pluggable job type executors on AzkabanExecutorServer, such as job
   types for hadoop ecosystem components.

We recommend installing these plugins for the best usage of Azkaban.
Below are instructions of how to install these plugins to work with
Azkaban.

User Manager Plugins
########

By default, Azkaban ships with the XMLUserManager class which
authenticates users based on a xml file, which is located at
``conf/azkaban-users.xml``.

This is not secure and doesn't serve many users. In real production
deployment, you should rely on your own user manager class that suits
your need, such as a LDAP based one. The ``XMLUserManager`` can still be
used for special user accounts and managing user roles. You can find
examples of these two cases in the default ``azkaban-users.xml`` file.

To install your own user manager class, specify in
``Azkaban-web-server-install-dir/conf/azkaban.properties``:

::

   user.manager.class=MyUserManagerClass

and put the containing jar in ``plugins`` directory.

Viewer Plugins
########

HDFS Viewer Plugin
**********************

HDFS Viewer Plugin should be installed in AzkabanWebServer plugins
directory, which is specified in AzkabanWebServer's config file, for
example, in ``Azkaban-web-server-install-dir/conf/azkaban.properties``:
::

   viewer.plugins=hdfs

This tells Azkaban to load hdfs viewer plugin from
``Azkaban-web-server-install-dir/plugins/viewer/hdfs``.

Extract the ``azkaban-hdfs-viewer`` archive to the AzkabanWebServer
``./plugins/viewer`` directory. Rename the directory to ``hdfs``, as
specified above.

Depending on if the hadoop installation is turned on:

#. If the Hadoop installation does not have security turned on, the
   default config is good enough. One can simply restart
   ``AzkabanWebServer`` and start using the HDFS viewer.
#. If the Hadoop installation does have security turned on, the
   following configs should be set differently than their default
   values, in plugin's config file:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| ``azkaban.should.proxy``          | Whether Azkaban should proxy as   |
|                                   | another user to view the hdfs     |
|                                   | filesystem, rather than Azkaban   |
|                                   | itself, defaults to ``true``      |
+-----------------------------------+-----------------------------------+
| ``hadoop.security.manager.class`` | The security manager to be used,  |
|                                   | which handles talking to secure   |
|                                   | hadoop cluster, defaults to       |
|                                   | ``azkaban.security.HadoopSecurity |
|                                   | Manager_H_1_0``                   |
|                                   | (for hadoop 1.x versions)         |
+-----------------------------------+-----------------------------------+
| ``proxy.user``                    | The Azkaban user configured with  |
|                                   | kerberos and hadoop. Similar to   |
|                                   | how oozie should be configured,   |
|                                   | for secure hadoop installations   |
+-----------------------------------+-----------------------------------+
| ``proxy.keytab.location``         | The location of the keytab file   |
|                                   | with which Azkaban can            |
|                                   | authenticate with Kerberos for    |
|                                   | the specified ``proxy.user``      |
+-----------------------------------+-----------------------------------+

For more Hadoop security related information, see
`HadoopSecurityManager <https://azkaban.github.io/azkaban/docs/latest/#hadoopsecuritymanager>`__

Job Type Plugins
########

Azkaban has a limited set of built-in job types to run local unix
commands and simple java programs. In most cases, you will want to
install additional job type plugins, for example, hadoopJava, Pig, Hive,
VoldemortBuildAndPush, etc. Some of the common ones are included in
azkaban-jobtype archive. Here is how to install:

Job type plugins should be installed with AzkabanExecutorServer's
plugins directory, and specified in AzkabanExecutorServer's config file.
For example, in
``Azkaban-exec-server-install-dir/conf/azkaban.properties``:

::

   azkaban.jobtype.plugin.dir=plugins/jobtypes

This tells Azkaban to load all job types from
``Azkaban-exec-server-install-dir/plugins/jobtypes``. Extract the
archive into AzkabanExecutorServer ``./plugins/`` directory, rename it
to ``jobtypes`` as specified above.

The following setting is often needed when you run Hadoop Jobs:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| ``hadoop.home``                   | Your ``$HADOOP_HOME`` setting.    |
+-----------------------------------+-----------------------------------+
| ``jobtype.global.classpath``      | The cluster specific hadoop       |
|                                   | resources, such as hadoop-core    |
|                                   | jar, and hadoop conf (e.g.        |
|                                   | ``${hadoop.home}/hadoop-core-1.0. |
|                                   | 4.jar,${hadoop.home}/conf``)      |
+-----------------------------------+-----------------------------------+

Depending on if the hadoop installation is turned on:

-  If the hadoop installation does not have security turned on, you can
   likely rely on the default settings.
-  If the Hadoop installation does have kerberos authentication turned
   on, you need to fill out the following hadoop settings:

+-----------------------------------+-----------------------------------+
| Parameter                         | Description                       |
+===================================+===================================+
| ``hadoop.security.manager.class`` | The security manager to be used,  |
|                                   | which handles talking to secure   |
|                                   | hadoop cluster, defaults to       |
|                                   | ``azkaban.security.HadoopSecurity |
|                                   | Manager_H_1_0``                   |
|                                   | (for hadoop 1.x versions)         |
+-----------------------------------+-----------------------------------+
| ``proxy.user``                    | The Azkaban user configured with  |
|                                   | kerberos and hadoop. Similar to   |
|                                   | how oozie should be configured,   |
|                                   | for secure hadoop installations   |
+-----------------------------------+-----------------------------------+
| ``proxy.keytab.location``         | The location of the keytab file   |
|                                   | with which Azkaban can            |
|                                   | authenticate with Kerberos for    |
|                                   | the specified proxy.user          |
+-----------------------------------+-----------------------------------+

For more Hadoop security related information, see
`HadoopSecurityManager <https://azkaban.github.io/azkaban/docs/latest/#hadoopsecuritymanager>`__

Finally, start the executor, watch for error messages and check executor
server log. For job type plugins, the executor should do minimum testing
and let you know if it is properly installed.

--------------

*****
Property Overrides
*****

Azkaban job is specified with a set of key-value pairs we call
properties. There are multiple sources for deciding which properties
will finally be a part of job execution. Following table lists out all
the sources of properties and their priorities. Please note that if a
property occur in multiple sources, then its value from high property
source will be used

Following properties are visible to the users. These are the same
properties which are merged to form ``jobProps`` in
``AbstractProcessJob.java``

+-----------------------+-----------------------+-----------------------+
| PropertySource        | Description           | Priority              |
+=======================+=======================+=======================+
| ``global.properties`` | These are admin       | Lowest (0)            |
| in ``conf`` directory | configured properties |                       |
|                       | during Azkaban setup. |                       |
|                       | Global to all         |                       |
|                       | jobtypes.             |                       |
+-----------------------+-----------------------+-----------------------+
| ``common.properties`` | These are admin       | 1                     |
| in ``jobtype``        | configured properties |                       |
| directory             | during Azkaban setup. |                       |
|                       | Global to all         |                       |
|                       | jobtypes.             |                       |
+-----------------------+-----------------------+-----------------------+
|``plugin.properties``  | These are admin       | 2                     |
| in                    | configured properties |                       |
| ``jobtype/<jobtype-na | during Azkaban setup. |                       |
| me>``                 | Restricted to a       |                       |
| directory             | specific jobtype.     |                       |
+-----------------------+-----------------------+-----------------------+
| ``common.properties`` | These are user        | 3                     |
| in project zip        | specified property    |                       |
|                       | which apply to all    |                       |
|                       | jobs in sibling or    |                       |
|                       | descendent            |                       |
|                       | directories           |                       |
+-----------------------+-----------------------+-----------------------+
| Flow properties       | These are user        | 4                     |
| specified while       | specified property.   |                       |
| triggering flow       | These can be          |                       |
| execution             | specified from UI or  |                       |
|                       | Ajax call but cannot  |                       |
|                       | be saved in project   |                       |
|                       | zip.                  |                       |
+-----------------------+-----------------------+-----------------------+
| ``{job-name}.job``    | These are user        | Highest (5)           |
| job specification     | specified property in |                       |
|                       | actual job file       |                       |
+-----------------------+-----------------------+-----------------------+

Following properties are not visible to the users. Depending on jobtype
implementation these properties are used for constraining user jobs and
properties. These are the same properties which are merged to form
``sysProps`` in ``AbstractProcessJob.java``

+-----------------------+-----------------------+-----------------------+
| PropertySource        | Description           | Priority              |
+=======================+=======================+=======================+
| commonprivate.prope   | These are admin       | Lowest (0)            |
|rties                  | configured properties |                       |
| in jobtype            | during Azkaban setup. |                       |
| directory             | Global to all         |                       |
|                       | jobtypes.             |                       |
+-----------------------+-----------------------+-----------------------+
| private.properties    | These are admin       | Highest (1)           |
|                       | configured properties |                       |
| in                    | during Azkaban setup. |                       |
|  jobtype/{jobtype-na  | Restricted to a       |                       |
|me}                   | specific jobtype.     |                       |
| directory             |                       |                       |
+-----------------------+-----------------------+-----------------------+

``azkaban.properties`` is another type of properties which are only used
for controlling Azkaban webserver and execserver configuration. Please
note that ``jobProps``, ``sysProps`` and ``azkaban.properties`` are 3
different types of properties and are not merged in general (depends on
jobtype implementation).

--------------

*****
Upgrading DB from 2.1
*****

If installing Azkaban from scratch, you can ignore this document. This
is only for those who are upgrading from 2.1 to 2.5.

The ``update_2.1_to_3.0.sql`` needs to be run to alter all the tables.
This includes several table alterations and a new table.

Here are the changes:

-  Alter project_properties table'

   -  Modify 'name' column to be 255 characters

-  Create new table triggers

Importing Existing Schedules from 2.1
########

In 3.0, the scheduling system is merged into the new triggering system.
The information will be persisted in ``triggers`` table in DB. We have a
simple tool to import your existing schedules into this new table.

After you download and install web server, please run this command
**once** from web server install directory:

::

   $ bash bin/schedule2trigger.sh

--------------

*****
Upgrading DB from 2.7.0
*****

If installing Azkaban from scratch, you can ignore this document. This
is only for those who are upgrading from 2.7 to 3.0.

The ``create.executors.sql``, ``update.active_executing_flows.3.0.sql``,
``update.execution_flows.3.0.sql``, and ``create.executor_events.sql``
needs to be run to alter all the tables. This includes several table
alterations and two new table.

Here are the changes:

-  Alter active_executing_flows table'

   -  Deleting 'port' column
   -  Deleting 'host' column

-  Alter execution_flows table'

   -  Adding an 'executor_id' column

-  Create new executors table
-  Create new executor events table

