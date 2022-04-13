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

Logging logs to a Kafka cluster
-----------------------------------

Azkaban supports sending job and flow logs to a log ingestion (such as ELK)
cluster via a Kafka appender. In order to enable this in Azkaban, you
will need to set three logging kafka properties (shown here with sample
values):

.. code-block:: guess

   azkaban.logging.kafka.brokers=localhost:9092
   azkaban.job.logging.kafka.topic=azkaban-job-logging
   azkaban.flow.logging.kafka.topic=azkaban-flow-logging

These configure where Azkaban can find your Kafka cluster, and also
which topic to put the logs under. Failure to provide these parameters
will result in Azkaban refusing to create a Kafka appender upon
requesting one.

In order to configure jobs and flows to send its logs to Kafka, the following
property needs to be set to true:

.. code-block:: guess

   azkaban.logging.kafka.enabled=true

By default, the logging kafka log4j appender is org.apache.kafka.log4jappender.KafkaLog4jAppender
 and logs will broadcast in JSON form to the Kafka cluster. It has the following
structure:

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

Instead of using the default kafka log4j appender, you can plug in your own logging kafka log4j
appender by extending org.apache.kafka.log4jappender.KafkaLog4jAppender and set the following
property to your appender:

.. code-block:: guess

   azkaban.logging.kafka.class=a.b.c.yourKafkaLog4jAppender