/*
 * Copyright 2016 LinkedIn Corp.
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
 */

package azkaban.constants;

public class ServerProperties {
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
  public static final String CUSTOM_METRICS_REPORTER_CLASS_NAME =
      "azkaban.metrics.reporter.name";

  // Represent the metrics server URL.
  public static final String METRICS_SERVER_URL =
      "azkaban.metrics.server.url";

  public static final String IS_METRICS_ENABLED =
      "azkaban.is.metrics.enabled";
}
