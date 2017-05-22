/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package azkaban;

/**
 * Constants
 *
 * Global place for storing constants.
 * Conventions:
 * - All internal constants to be put in the root level ie. {@link Constants} class
 * - All Configuration keys to be put in {@link ConfigurationKeys} class
 * - Flow level Properties keys go to {@link FlowProperties}
 * - Job  level Properties keys go to {@link JobProperties}
 */
public class Constants {

  // Names and paths of various file names to configure Azkaban
  public static final String AZKABAN_PROPERTIES_FILE = "azkaban.properties";
  public static final String AZKABAN_PRIVATE_PROPERTIES_FILE = "azkaban.private.properties";
  public static final String DEFAULT_CONF_PATH = "conf";
  public static final String AZKABAN_EXECUTOR_PORT_FILENAME = "executor.port";

  public static final String AZKABAN_SERVLET_CONTEXT_KEY = "azkaban_app";

  // Internal username used to perform SLA action
  public static final String AZKABAN_SLA_CHECKER_USERNAME = "azkaban_sla";

  // Memory check retry interval when OOM in ms
  public static final long MEMORY_CHECK_INTERVAL_MS = 1000 * 60 * 1;

  // Max number of memory check retry
  public static final int MEMORY_CHECK_RETRY_LIMIT = 720;
  public static final int DEFAULT_PORT_NUMBER = 8081;
  public static final int DEFAULT_SSL_PORT_NUMBER = 8443;
  public static final int DEFAULT_JETTY_MAX_THREAD_COUNT = 20;

  // One Schedule's default End Time: 01/01/2050, 00:00:00, UTC
  public static final long DEFAULT_SCHEDULE_END_EPOCH_TIME = 2524608000000L;

  public static class ConfigurationKeys {
    // These properties are configurable through azkaban.properties

    // Defines a list of external links, each referred to as a topic
    public static final String AZKABAN_SERVER_EXTERNAL_TOPICS = "azkaban.server.external.topics";

    // External URL template of a given topic, specified in the list defined above
    public static final String AZKABAN_SERVER_EXTERNAL_TOPIC_URL = "azkaban.server.external.${topic}.url";

    // Designates one of the external link topics to correspond to an execution analyzer
    public static final String AZKABAN_SERVER_EXTERNAL_ANALYZER_TOPIC = "azkaban.server.external.analyzer.topic";
    public static final String AZKABAN_SERVER_EXTERNAL_ANALYZER_LABEL = "azkaban.server.external.analyzer.label";

    // Designates one of the external link topics to correspond to a job log viewer
    public static final String AZKABAN_SERVER_EXTERNAL_LOGVIEWER_TOPIC = "azkaban.server.external.logviewer.topic";
    public static final String AZKABAN_SERVER_EXTERNAL_LOGVIEWER_LABEL = "azkaban.server.external.logviewer.label";

    // Configures the Kafka appender for logging user jobs, specified for the exec server
    public static final String AZKABAN_SERVER_LOGGING_KAFKA_BROKERLIST = "azkaban.server.logging.kafka.brokerList";
    public static final String AZKABAN_SERVER_LOGGING_KAFKA_TOPIC = "azkaban.server.logging.kafka.topic";

    // Represent the class name of azkaban metrics reporter.
    public static final String CUSTOM_METRICS_REPORTER_CLASS_NAME = "azkaban.metrics.reporter.name";

    // Represent the metrics server URL.
    public static final String METRICS_SERVER_URL = "azkaban.metrics.server.url";

    public static final String IS_METRICS_ENABLED = "azkaban.is.metrics.enabled";

    // Hostname for the host, if not specified, canonical hostname will be used
    public static final String AZKABAN_SERVER_HOST_NAME = "azkaban.server.hostname";

    // Legacy configs section, new configs should follow the naming convention of azkaban.server.<rest of the name> for server configs.

    // The property is used for the web server to get the host name of the executor when running in SOLO mode.
    public static final String EXECUTOR_HOST = "executor.host";

    // Max flow running time in mins, server will kill flows running longer than this setting.
    // if not set or <= 0, then there's no restriction on running time.
    public static final String AZKABAN_MAX_FLOW_RUNNING_MINS = "azkaban.server.flow.max.running.minutes";

    public static final String AZKABAN_STORAGE_TYPE = "azkaban.storage.type";
    public static final String AZKABAN_STORAGE_LOCAL_BASEDIR = "azkaban.storage.local.basedir";
    public static final String HADOOP_CONF_DIR_PATH = "hadoop.conf.dir.path";
    public static final String AZKABAN_STORAGE_HDFS_ROOT_URI = "azkaban.storage.hdfs.root.uri";
    public static final String AZKABAN_KERBEROS_PRINCIPAL = "azkaban.kerberos.principal";
    public static final String AZKABAN_KEYTAB_PATH = "azkaban.keytab.path";
  }

  public static class FlowProperties {

    // Basic properties of flows as set by the executor server
    public static final String AZKABAN_FLOW_PROJECT_NAME = "azkaban.flow.projectname";
    public static final String AZKABAN_FLOW_FLOW_ID = "azkaban.flow.flowid";
    public static final String AZKABAN_FLOW_SUBMIT_USER = "azkaban.flow.submituser";
    public static final String AZKABAN_FLOW_EXEC_ID = "azkaban.flow.execid";
    public static final String AZKABAN_FLOW_PROJECT_VERSION = "azkaban.flow.projectversion";
  }

  public static class JobProperties {

    // Job property that enables/disables using Kafka logging of user job logs
    public static final String AZKABAN_JOB_LOGGING_KAFKA_ENABLE = "azkaban.job.logging.kafka.enable";
  }

  public static class JobCallbackProperties {

    public static final String JOBCALLBACK_CONNECTION_REQUEST_TIMEOUT = "jobcallback.connection.request.timeout";
    public static final String JOBCALLBACK_CONNECTION_TIMEOUT = "jobcallback.connection.timeout";
    public static final String JOBCALLBACK_SOCKET_TIMEOUT = "jobcallback.socket.timeout";
    public static final String JOBCALLBACK_RESPONSE_WAIT_TIMEOUT = "jobcallback.response.wait.timeout";
    public static final String JOBCALLBACK_THREAD_POOL_SIZE = "jobcallback.thread.pool.size";
  }
}
