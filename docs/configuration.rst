.. _configs:


Configurations
==================================

Azkaban can be configured in many ways. The following describes the knobs and switches that can be set. For the most part,
there is no need to deviate from the default values.


*****
Azkaban Web Server Configurations
*****

These are properties to configure the web server. They should be set in ``azkaban.properties``.


General Properties
########

+-----------------------+-----------------------+-----------------------+
| Parameter             | Description           | Default               |
+=======================+=======================+=======================+
|   azkaban.name        | The name of the       | Local                 |
|                       | azkaban instance that |                       |
|                       | will show up in the   |                       |
|                       | UI. Useful if you run |                       |
|                       | more than one Azkaban |                       |
|                       | instance.             |                       |
+-----------------------+-----------------------+-----------------------+
|   azkaban.label       | A label to describe   | My Local Azkaban      |
|                       | the Azkaban instance. |                       |
+-----------------------+-----------------------+-----------------------+
|   azkaban.color       | Hex value that allows | #FF3601               |
|                       | you to set a style    |                       |
|                       | color for the Azkaban |                       |
|                       | UI.                   |                       |
+-----------------------+-----------------------+-----------------------+
|   azkaban.depth       | Graph expansion level.| 2                     |
|                       | Zero (0) means all    | This means the flows  |
|                       | flows are collapsed   | will be recursively   |
|                       | when the graph is     | expanded up to two    |
|                       | rendered for the      | levels down when the  |
|                       | first time.           | graph is 1st shown.   |
+-----------------------+-----------------------+-----------------------+
|   web.resource.dir    | Sets the directory    | web/                  |
|                       | for the ui’s css and  |                       |
|                       | javascript files.     |                       |
+-----------------------+-----------------------+-----------------------+
|   default.timezone    | The timezone that     | America/Los_Angeles   |
|                       | will be displayed by  |                       |
|                       | Azkaban.              |                       |
+-----------------------+-----------------------+-----------------------+
|   viewer.plugin.dir   | Directory where       | plugins/viewer        |
|                       | viewer plugins are    |                       |
|                       | installed.            |                       |
+-----------------------+-----------------------+-----------------------+
|   job.max.Xms         | The maximum initial   | 1GB                   |
|                       | amount of memory each |                       |
|                       | job can request. This |                       |
|                       | validation is         |                       |
|                       | performed at project  |                       |
|                       | upload time           |                       |
+-----------------------+-----------------------+-----------------------+
|   job.max.Xmx         | The maximum amount of | 2GB                   |
|                       | memory each job can   |                       |
|                       | request. This         |                       |
|                       | validation is         |                       |
|                       | performed at project  |                       |
|                       | upload time           |                       |
+-----------------------+-----------------------+-----------------------+

Multiple Executor Mode Parameters
########

+------------------------------------------------------+-----------------------+-----------------------+
| Parameter                                            | Description           | Default               |
+======================================================+=======================+=======================+
| azkaban.use.multiple.executors                       | Should azkaban run in | false                 |
|                                                      | multi-executor mode.  |                       |
|                                                      | Required for multiple |                       |
|                                                      | executor mode.        |                       |
+------------------------------------------------------+-----------------------+-----------------------+
| azkaban.executorselector.filters                     | A common separated    |                       |
|                                                      | list of hard filters  |                       |
|                                                      | to be used while      |                       |
|                                                      | dispatching. To be    |                       |
|                                                      | chosen from          |                       |
|                                                      | StaticRemaining,      |                       |
|                                                      | FlowSize,             |                       |
|                                                      | MinimumFreeMemory and |                       |
|                                                      | CpuStatus. Order of   |                       |
|                                                      | filter does not       |                       |
|                                                      | matter.               |                       |
+------------------------------------------------------+-----------------------+-----------------------+
| azkaban.executorselector.comparator.{ComparatorName} | Integer weight to be  |                       |
|                                                      | used to rank          |                       |
|                                                      | available executors   |                       |
|                                                      | for a given flow.     |                       |
|                                                      | Currently,            |                       |
|                                                      | {ComparatorName} can  |                       |
|                                                      | be                    |                       |
|                                                      | NumberOfAssignedFlowC |                       |
|                                                      | omparator,            |                       |
|                                                      | Memory,               |                       |
|                                                      | LastDispatched and    |                       |
|                                                      | CpuUsage as           |                       |
|                                                      | ComparatorName. For   |                       |
|                                                      | example:-             |                       |
|                                                      | azkaban.executorselec |                       |
|                                                      | tor.comparator.Memory |                       |
|                                                      | =2                    |                       |
+------------------------------------------------------+-----------------------+-----------------------+
| azkaban.queueprocessing.enabled                      | Should queue          | true                  |
|                                                      | processor be enabled  |                       |
|                                                      | from webserver        |                       |
|                                                      | initialization        |                       |
+------------------------------------------------------+-----------------------+-----------------------+
| azkaban.webserver.queue.size                         | Maximum flows that    | 100000                |
|                                                      | can be queued at      |                       |
|                                                      | webserver             |                       |
+------------------------------------------------------+-----------------------+-----------------------+
| azkaban.activeexecutor.refresh.milisecinterval       | Maximum time in       | 50000                 |
|                                                      | milliseconds that can |                       |
|                                                      | be processed without  |                       |
|                                                      | executor statistics   |                       |
|                                                      | refresh               |                       |
+------------------------------------------------------+-----------------------+-----------------------+
| azkaban.activeexecutor.refresh.flowinterval          | Maximum number of     | 5                     |
|                                                      | queued flows that can |                       |
|                                                      | be processed without  |                       |
|                                                      | executor statistics   |                       |
|                                                      | refresh               |                       |
+------------------------------------------------------+-----------------------+-----------------------+
| azkaban.executorinfo.refresh.maxThreads              | Maximum number of     | 5                     |
|                                                      | threads to refresh    |                       |
|                                                      | executor statistics   |                       |
+------------------------------------------------------+-----------------------+-----------------------+

Jetty Parameters
########

+---------------------+---------------------+---------+
| Parameter           | Description         | Default |
+=====================+=====================+=========+
| jetty.maxThreads    | Max request threads | 25      |
+---------------------+---------------------+---------+
| jetty.ssl.port      | The ssl port        | 8443    |
+---------------------+---------------------+---------+
| jetty.keystore      | The keystore file   |         |
+---------------------+---------------------+---------+
| jetty.password      | The jetty password  |         |
+---------------------+---------------------+---------+
| jetty.keypassword   | The keypassword     |         |
+---------------------+---------------------+---------+
| jetty.truststore    | The trust store     |         |
+---------------------+---------------------+---------+
| jetty.trustpassword | The trust password  |         |
+---------------------+---------------------+---------+

Project Manager Settings
########

+---------------------------+-----------------------+-----------------------+
| Parameter                 | Description           | Default               |
+===========================+=======================+=======================+
| project.temp.dir          | The temporary         | temp                  |
|                           | directory used when   |                       |
|                           | uploading projects    |                       |
+---------------------------+-----------------------+-----------------------+
| project.version.retention | The number of unused  | 3                     |
|                           | project versions      |                       |
|                           | retained before       |                       |
|                           | cleaning              |                       |
+---------------------------+-----------------------+-----------------------+
| creator.default.proxy     | Auto add the creator  | true                  |
|                           | of the projects as a  |                       |
|                           | proxy user to the     |                       |
|                           | project.              |                       |
+---------------------------+-----------------------+-----------------------+
| lockdown.create.projects  | Prevents anyone       | false                 |
|                           | except those with     |                       |
|                           | Admin roles to create |                       |
|                           | new projects.         |                       |
+---------------------------+-----------------------+-----------------------+
| lockdown.upload.projects  | Prevents anyone but   | false                 |
|                           | admin users and users |                       |
|                           | with permissions to   |                       |
|                           | upload projects.      |                       |
+---------------------------+-----------------------+-----------------------+

MySQL Connection Parameter
########

+-----------------------+-----------------------+-----------------------+
| Parameter             | Description           | Default               |
+=======================+=======================+=======================+
| database.type         | The database type.    | mysql                 |
|                       | Currently, the only   |                       |
|                       | database supported is |                       |
|                       | mysql.                |                       |
+-----------------------+-----------------------+-----------------------+
| mysql.port            | The port to the mysql | 3306                  |
|                       | db                    |                       |
+-----------------------+-----------------------+-----------------------+
| mysql.host            | The mysql host        | localhost             |
+-----------------------+-----------------------+-----------------------+
| mysql.database        | The mysql database    |                       |
+-----------------------+-----------------------+-----------------------+
| mysql.user            | The mysql user        |                       |
+-----------------------+-----------------------+-----------------------+
| mysql.password        | The mysql password    |                       |
+-----------------------+-----------------------+-----------------------+
| mysql.numconnections  | The number of         | 100                   |
|                       | connections that      |                       |
|                       | Azkaban web client    |                       |
|                       | can open to the       |                       |
|                       | database              |                       |
+-----------------------+-----------------------+-----------------------+

Executor Manager Properties
########

+-----------------------------+-----------------------+-----------------------+
| Parameter                   | Description           | Default               |
+=============================+=======================+=======================+
| execution.logs.retention.ms | Time in milliseconds  | 7257600000L (12       |
|                             | that execution logs   | weeks)                |
|                             | are retained          |                       |
+-----------------------------+-----------------------+-----------------------+

Notification Email Properties
########

+---------------+-----------------------------------------------------+---------+
| Parameter     | Description                                         | Default |
+===============+=====================================================+=========+
| mail.sender   | The email address that azkaban uses to send emails. |         |
+---------------+-----------------------------------------------------+---------+
| mail.host     | The email server host machine.                      |         |
+---------------+-----------------------------------------------------+---------+
| mail.user     | The email server user name.                         |         |
+---------------+-----------------------------------------------------+---------+
| mail.password | The email password user name.                       |         |
+---------------+-----------------------------------------------------+---------+

User Manager Properties
########

+-----------------------+-----------------------+-----------------------+
| Parameter             | Description           | Default               |
+=======================+=======================+=======================+
| user.manager.class    | The user manager that | azkaban.user.XmlUserM |
|                       | is used to            | anager                |
|                       | authenticate a user.  |                       |
|                       | The default is an XML |                       |
|                       | user manager, but it  |                       |
|                       | can be overwritten to |                       |
|                       | support other         |                       |
|                       | authentication        |                       |
|                       | methods, such as      |                       |
|                       | JDNI.                 |                       |
+-----------------------+-----------------------+-----------------------+
| user.manager.xml.file | Xml file for the      | conf/azkaban-users.xm |
|                       | XmlUserManager        | l                     |
+-----------------------+-----------------------+-----------------------+

User Session Properties
########

+-----------------------+-----------------------+-----------------------+
| Parameter             | Description           | Default               |
+=======================+=======================+=======================+
| session.time.to.live  | The session time to   | 86400000              |
|                       | live in ms seconds    |                       |
+-----------------------+-----------------------+-----------------------+
| max.num.sessions      | The maximum number of | 10000                 |
|                       | sessions before       |                       |
|                       | people are evicted.   |                       |
+-----------------------+-----------------------+-----------------------+

*****
Azkaban Executor Server Configuration
*****

Executor Server Properties
########

+-------------------------------------------+-----------------------+-----------------------+
| Parameter                                 | Description           | Default               |
+===========================================+=======================+=======================+
| executor.port                             | The port for azkaban  | 0 (any free port)     |
|                                           | executor server       |                       |
+-------------------------------------------+-----------------------+-----------------------+
| executor.global.properties                | A path to the         |   none                |
|                                           | properties that will  |                       |
|                                           | be the parent for all |                       |
|                                           | jobs.                 |                       |
+-------------------------------------------+-----------------------+-----------------------+
| azkaban.execution.dir                     | The folder for        | executions            |
|                                           | executing working     |                       |
|                                           | directories           |                       |
+-------------------------------------------+-----------------------+-----------------------+
| azkaban.project.dir                       | The folder for        | projects              |
|                                           | storing temporary     |                       |
|                                           | copies of project     |                       |
|                                           | files used for        |                       |
|                                           | executions            |                       |
+-------------------------------------------+-----------------------+-----------------------+
| executor.flow.threads                     | The number of         | 30                    |
|                                           | simultaneous flows     |                       |
|                                           | that can be run.      |                       |
|                                           | These threads are     |                       |
|                                           | mostly idle.          |                       |
+-----------------------+-----------------------+-----------------------+
| job.log.chunk.size                        | For rolling job logs. | 5MB                   |
|                                           | The chuck size for    |                       |
|                                           | each roll over        |                       |
+-------------------------------------------+-----------------------+-----------------------+
| job.log.backup.index                      | The number of log     | 4                     |
|                                           | chunks. The max size  |                       |
|                                           | of each log is then  |                       |
|                                           | the index \*          |                       |
|                                           | chunksize             |                       |
+-------------------------------------------+-----------------------+-----------------------+
| flow.num.job.threads                      | The number of         | 10                    |
|                                           | concurrent running    |                       |
|                                           | jobs in each flow.    |                       |
|                                           | These threads are     |                       |
|                                           | mostly idle.          |                       |
+-------------------------------------------+-----------------------+-----------------------+
|   job.max.Xms                             | The maximum initial   | 1GB                   |
|                                           | amount of memory each |                       |
|                                           | job can request. If a |                       |
|                                           | job requests more     |                       |
|                                           | than this, then       |                       |
|                                           | Azkaban server will   |                       |
|                                           | not launch this job   |                       |
+-------------------------------------------+-----------------------+-----------------------+
|   job.max.Xmx                             | The maximum amount of | 2GB                   |
|                                           | memory each job can   |                       |
|                                           | request. If a job     |                       |
|                                           | requests more than    |                       |
|                                           | this, then Azkaban    |                       |
|                                           | server will not       |                       |
|                                           | launch this job       |                       |
+-------------------------------------------+-----------------------+-----------------------+
|   azkaban.server.flow.max.running.minutes | The maximum time in   | -1                    |
|                                           | minutes a flow will   |                       |
|                                           | be living inside      |                       |
|                                           | azkaban after being   |                       |
|                                           | executed. If a flow   |                       |
|                                           | runs longer than      |                       |
|                                           | this, it will be      |                       |
|                                           | killed. If smaller or |                       |
|                                           | equal to 0, there's   |                       |
|                                           | no restriction on     |                       |
|                                           | running time.         |                       |
+-------------------------------------------+-----------------------+-----------------------+


MySQL Connection Parameter
########

+-----------------------+-----------------------+-----------------------+
| Parameter             | Description           | Default               |
+=======================+=======================+=======================+
|  database.type        | The database type.    | mysql                 |
|                       | Currently, the only   |                       |
|                       | database supported is |                       |
|                       | mysql.                |                       |
+-----------------------+-----------------------+-----------------------+
|  mysql.port           | The port to the mysql | 3306                  |
|                       | db                    |                       |
+-----------------------+-----------------------+-----------------------+
|  mysql.host           | The mysql host        | localhost             |
+-----------------------+-----------------------+-----------------------+
|  mysql.database       | The mysql database    |                       |
+-----------------------+-----------------------+-----------------------+
|  mysql.user           | The mysql user        |                       |
+-----------------------+-----------------------+-----------------------+
|  mysql.password       | The mysql password    |                       |
+-----------------------+-----------------------+-----------------------+
|  mysql.numconnections | The number of         | 100                   |
|                       | connections that      |                       |
|                       | Azkaban web client    |                       |
|                       | can open to the       |                       |
|                       | database              |                       |
+-----------------------+-----------------------+-----------------------+


*****
Plugin Configurations
*****


Execute-As-User
########

With a new security enhancement in Azkaban 3.0, Azkaban jobs can now run
as the submit user or the user.to.proxy of the flow by default. This
ensures that Azkaban takes advantage of the Linux permission security
mechanism, and operationally this simplifies resource monitoring and
visibility. Set up this behavior by doing the following:-

Execute.as.user is set to true by default. In case needed, it can also
be configured to false in azkaban-plugin’s commonprivate.properties
Configure azkaban.native.lib= to the place where you are going to put
the compiled execute-as-user.c file (see below)
Generate an executable on the Azkaban box for
azkaban-common/src/main/c/execute-as-user.c. **it should be named
execute-as-user** Below is a sample approach

-  ``scp ./azkaban-common/src/main/c/execute-as-user.c`` onto the
   Azkaban box
-  run: ``gcc execute-as-user.c -o execute-as-user``
-  run: ``chown root execute-as-user (you might need root privilege)``
-  run: ``chmod 6050 execute-as-user (you might need root privilege)``
