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

public class ServerInternals {
  // Constants pertaining to the internal running of the Azkaban server

  // Names and paths of various file names to configure Azkaban
  public static final String AZKABAN_PROPERTIES_FILE = "azkaban.properties";
  public static final String AZKABAN_PRIVATE_PROPERTIES_FILE = "azkaban.private.properties";
  public static final String DEFAULT_CONF_PATH = "conf";
  public static final String AZKABAN_EXECUTOR_PORT_FILENAME = "executor.port";

  public static final String AZKABAN_SERVLET_CONTEXT_KEY = "azkaban_app";
  
}
