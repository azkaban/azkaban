/*
 * Copyright 2018 LinkedIn Corp.
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
package azkaban.jobtype;

import org.apache.hadoop.conf.Configuration;

import azkaban.utils.Props;


/**
 * HadoopTuningConfigurationInjector is responsible for inserting links back to the
 * Azkaban UI in configurations and for automatically injecting designated job
 * properties into the Hadoop configuration.
 * <p>
 * It is assumed that the necessary links have already been loaded into the
 * properties. After writing the necessary links as a xml file as required by
 * Hadoop's configuration, clients may add the links as a default resource
 * using injectResources() so that they are included in any Configuration
 * constructed.
 */
public class HadoopTuningConfigurationInjector {

  // File to which the Hadoop configuration to inject will be written.
  public static final String INJECT_TUNING_FILE = "hadoop-tuning-inject.xml";

  /*
   * To be called by the forked process to load the generated links and Hadoop
   * configuration properties to automatically inject.
   *
   * @param props The Azkaban properties
   */
  public static void injectResources(Props props) {
    HadoopConfigurationInjector.injectResources(props);
    Configuration.addDefaultResource(INJECT_TUNING_FILE);
  }

  /**
   * Writes out the XML configuration file that will be injected by the client
   * as a configuration resource.
   * <p>
   * This file will include a series of links injected by Azkaban as well as
   * any job properties that begin with the designated injection prefix.
   *
   * @param props The Azkaban properties
   * @param workingDir The Azkaban job working directory
   */
  public static void prepareResourcesToInject(Props props, String workingDir) {
    HadoopConfigurationInjector.prepareResourcesToInject(props, workingDir, INJECT_TUNING_FILE);
  }

}
