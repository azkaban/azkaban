/*
 * Copyright 2014 LinkedIn Corp.
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

import java.util.HashMap;
import java.util.Map;

import azkaban.jobExecutor.Job;
import azkaban.utils.Props;

/**
 * Container for job type plugins
 *
 * This contains the jobClass objects, the properties for loading plugins, and
 * the properties given by default to the plugin.
 *
 * This class is not thread safe, so adding to this class should only be
 * populated and controlled by the JobTypeManager
 */
public class JobTypePluginSet {
  private Map<String, Class<? extends Job>> jobToClass;
  private Map<String, Props> pluginJobPropsMap;
  private Map<String, Props> pluginLoadPropsMap;

  private Props commonJobProps;
  private Props commonLoadProps;

  /**
   * Base constructor
   */
  public JobTypePluginSet() {
    jobToClass = new HashMap<String, Class<? extends Job>>();
    pluginJobPropsMap = new HashMap<String, Props>();
    pluginLoadPropsMap = new HashMap<String, Props>();
  }

  /**
   * Copy constructor
   *
   * @param clone
   */
  public JobTypePluginSet(JobTypePluginSet clone) {
    jobToClass = new HashMap<String, Class<? extends Job>>(clone.jobToClass);
    pluginJobPropsMap = new HashMap<String, Props>(clone.pluginJobPropsMap);
    pluginLoadPropsMap = new HashMap<String, Props>(clone.pluginLoadPropsMap);
    commonJobProps = clone.commonJobProps;
    commonLoadProps = clone.commonLoadProps;
  }

  /**
   * Sets the common properties shared in every jobtype
   *
   * @param commonJobProps
   */
  public void setCommonPluginJobProps(Props commonJobProps) {
    this.commonJobProps = commonJobProps;
  }

  /**
   * Sets the common properties used to load every plugin
   *
   * @param commonLoadProps
   */
  public void setCommonPluginLoadProps(Props commonLoadProps) {
    this.commonLoadProps = commonLoadProps;
  }

  /**
   * Gets common properties for every jobtype
   *
   * @return
   */
  public Props getCommonPluginJobProps() {
    return commonJobProps;
  }

  /**
   * Gets the common properties used to load a plugin
   *
   * @return
   */
  public Props getCommonPluginLoadProps() {
    return commonLoadProps;
  }

  /**
   * Get the properties for a jobtype used to setup and load a plugin
   *
   * @param jobTypeName
   * @return
   */
  public Props getPluginLoaderProps(String jobTypeName) {
    return pluginLoadPropsMap.get(jobTypeName);
  }

  /**
   * Get the properties that will be given to the plugin as default job
   * properties.
   *
   * @param jobTypeName
   * @return
   */
  public Props getPluginJobProps(String jobTypeName) {
    return pluginJobPropsMap.get(jobTypeName);
  }

  /**
   * Gets the plugin job runner class
   *
   * @param jobTypeName
   * @return
   */
  public Class<? extends Job> getPluginClass(String jobTypeName) {
    return jobToClass.get(jobTypeName);
  }

  /**
   * Adds plugin jobtype class
   */
  public void addPluginClass(String jobTypeName,
      Class<? extends Job> jobTypeClass) {
    jobToClass.put(jobTypeName, jobTypeClass);
  }

  /**
   * Adds plugin job properties used as default runtime properties
   */
  public void addPluginJobProps(String jobTypeName, Props props) {
    pluginJobPropsMap.put(jobTypeName, props);
  }

  /**
   * Adds plugin load properties used to load the plugin
   */
  public void addPluginLoadProps(String jobTypeName, Props props) {
    pluginLoadPropsMap.put(jobTypeName, props);
  }
}
