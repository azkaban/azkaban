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

import azkaban.jobExecutor.Job;
import azkaban.utils.Props;
import java.util.HashMap;
import java.util.Map;

/**
 * Container for job type plugins
 *
 * This contains the jobClass objects, the properties for loading plugins, and the properties given
 * by default to the plugin.
 *
 * This class is not thread safe, so adding to this class should only be populated and controlled by
 * the JobTypeManager
 */
public class JobTypePluginSet {

  private final Map<String, Class<? extends Job>> jobToClass;
  private final Map<String, Props> pluginJobPropsMap;
  private final Map<String, Props> pluginLoadPropsMap;
  private final Map<String, Props> pluginPrivatePropsMap;
  private final Map<String, JobPropsProcessor> pluginJobPropsProcessor;

  private Props commonJobProps;
  private Props commonLoadProps;

  /**
   * Base constructor
   */
  public JobTypePluginSet() {
    this.jobToClass = new HashMap<>();
    this.pluginJobPropsMap = new HashMap<>();
    this.pluginLoadPropsMap = new HashMap<>();
    this.pluginPrivatePropsMap = new HashMap<>();
    this.pluginJobPropsProcessor = new HashMap<>();
  }

  /**
   * Copy constructor
   */
  public JobTypePluginSet(final JobTypePluginSet clone) {
    this.jobToClass = new HashMap<>(clone.jobToClass);
    this.pluginJobPropsMap = new HashMap<>(clone.pluginJobPropsMap);
    this.pluginLoadPropsMap = new HashMap<>(clone.pluginLoadPropsMap);
    this.pluginPrivatePropsMap = new HashMap<>(clone.pluginPrivatePropsMap);
    this.commonJobProps = clone.commonJobProps;
    this.commonLoadProps = clone.commonLoadProps;
    this.pluginJobPropsProcessor = clone.pluginJobPropsProcessor;
  }

  /**
   * Gets common properties for every jobtype
   */
  public Props getCommonPluginJobProps() {
    return this.commonJobProps;
  }

  /**
   * Sets the common properties shared in every jobtype
   */
  public void setCommonPluginJobProps(final Props commonJobProps) {
    this.commonJobProps = commonJobProps;
  }

  /**
   * Gets the common properties used to load a plugin
   */
  public Props getCommonPluginLoadProps() {
    return this.commonLoadProps;
  }

  /**
   * Sets the common properties used to load every plugin
   */
  public void setCommonPluginLoadProps(final Props commonLoadProps) {
    this.commonLoadProps = commonLoadProps;
  }

  /**
   * Get the properties for a jobtype used to setup and load a plugin
   */
  public Props getPluginLoaderProps(final String jobTypeName) {
    return this.pluginLoadPropsMap.get(jobTypeName);
  }

  /**
   * Get the plugin private properties for the jobtype
   */
  public Props getPluginPrivateProps(final String jobTypeName) {
    return this.pluginPrivatePropsMap.get(jobTypeName);
  }
  /**
   * Get the properties that will be given to the plugin as default job properties.
   */
  public Props getPluginJobProps(final String jobTypeName) {
    return this.pluginJobPropsMap.get(jobTypeName);
  }

  /**
   * Gets the plugin job runner class
   */
  public Class<? extends Job> getPluginClass(final String jobTypeName) {
    return this.jobToClass.get(jobTypeName);
  }

  /**
   * Adds plugin jobtype class
   */
  public void addPluginClass(final String jobTypeName,
      final Class<? extends Job> jobTypeClass) {
    this.jobToClass.put(jobTypeName, jobTypeClass);
  }

  /**
   * Adds plugin job properties used as default runtime properties
   */
  public void addPluginJobProps(final String jobTypeName, final Props props) {
    this.pluginJobPropsMap.put(jobTypeName, props);
  }

  /**
   * Adds plugin load properties used to load the plugin
   */
  public void addPluginLoadProps(final String jobTypeName, final Props props) {
    this.pluginLoadPropsMap.put(jobTypeName, props);
  }

  /**
   * Adds plugins private properties used by the plugin
   */
  public void addPluginPrivateProps(final String jobTypeName, final Props props) {
    this.pluginPrivatePropsMap.put(jobTypeName, props);
  }

  public JobPropsProcessor getPluginJobPropsProcessor(
      final String jobTypeName) {
    return this.pluginJobPropsProcessor.get(jobTypeName);
  }

  public void addPluginJobPropsProcessor(final String jobTypeName,
      JobPropsProcessor jobPropsProcessor) {
    this.pluginJobPropsProcessor.put(jobTypeName, jobPropsProcessor);
  }
}
