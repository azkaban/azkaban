.. _how-to:

How Tos
=======

Force execution to an executor
------------------------------

Only users with admin privileges can use this override. In flow params:
set ``"useExecutor" = EXECUTOR_ID``.

Setting flow priority in multiple executor mode
-----------------------------------------------

Only users with admin privileges can use this property. In flow params:
set ``"flowPriority" = PRIORITY``. Higher numbers get executed first.

Enabling and Disabling Queue in multiple executor mode
------------------------------------------------------

Only users with admin privileges can use this action. Use curl or simply
visit following URL:-

-  Enable: ``WEBSERVER_URL/executor?ajax=disableQueueProcessor``
-  Disable: ``WEBSERVER_URL/executor?ajax=enableQueueProcessor``

Reloading executors in multiple executor mode
---------------------------------------------

Only users with admin privileges can use this action. This action need
at least one active executor to be successful. Use curl or simply visit
following URL:- ``WEBSERVER_URL/executor?ajax=reloadExecutors``

Logging job logs to a Kafka cluster
-----------------------------------

Azkaban supports sending job logs to a log ingestion (such as ELK)
cluster via a Kafka appender. In order to enable this in Azkaban, you
will need to set two exec server properties (shown here with sample
values):

.. code-block:: guess

   azkaban.server.logging.kafka.brokerList=localhost:9092
   azkaban.server.logging.kafka.topic=azkaban-logging

These configure where Azkaban can find your Kafka cluster, and also
which topic to put the logs under. Failure to provide these parameters
will result in Azkaban refusing to create a Kafka appender upon
requesting one.

In order to configure a job to send its logs to Kafka, the following job
property needs to be set to true:

.. code-block:: guess

   azkaban.job.logging.kafka.enable=true

Jobs with this setting enabled will broadcast its log messages in JSON
form to the Kafka cluster. It has the following structure:

.. code-block:: guess

   {
     "projectname": "Project name",
     "level": "INFO or ERROR",
     "submituser": "Someone",
     "projectversion": "Project version",
     "category": "Class name",
     "message": "Some log message",
     "logsource": "userJob",
     "flowid": "ID of flow",
     "execid": "ID of execution"
   }
