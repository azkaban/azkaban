/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.server;

public class Constants {
	public static final String AZKABAN_SERVLET_CONTEXT_KEY = "azkaban_app";
	public static final String AZKABAN_PROPERTIES_FILE = "azkaban.properties";
	public static final String AZKABAN_PRIVATE_PROPERTIES_FILE = "azkaban.private.properties";
	public static final String DEFAULT_CONF_PATH = "conf";
	public static final String AZKABAN_EXECUTOR_PORT_FILENAME = "executor.port";

  public static final String AZKABAN_SERVER_LOGGING_KAFKA_GLOBAL_DISABLE = "azkaban.logging.kafka.globalDisable";
  public static final String AZKABAN_SERVER_LOGGING_KAFKA_BROKERLIST = "azkaban.logging.kafka.brokerList";
  public static final String AZKABAN_SERVER_LOGGING_KAFKA_TOPIC = "azkaban.logging.kafka.topic";

  public static final String AZKABAN_FLOW_PROJECT_NAME = "azkaban.flow.projectname";
  public static final String AZKABAN_FLOW_FLOW_ID = "azkaban.flow.flowid";
  public static final String AZKABAN_FLOW_SUBMIT_USER = "azkaban.flow.submituser";
  public static final String AZKABAN_FLOW_EXEC_ID = "azkaban.flow.execid";
  public static final String AZKABAN_FLOW_PROJECT_VERSION = "azkaban.flow.projectversion";

  public static final String AZKABAN_JOB_LOGGING_KAFKA_ENABLE = "azkaban.job.logging.kafka.enable";
}
