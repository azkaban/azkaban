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

import azkaban.utils.Props;

import java.net.URL;
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
  private static final URL[] EMPTY_URLS = new URL[0];
  private final Map<String, Props> pluginJobPropsMap;
  private final Map<String, Props> pluginLoadPropsMap;
  private final Map<String, Props> pluginPrivatePropsMap;
  private final Map<String, JobPropsProcessor> pluginJobPropsProcessor;
  private final Map<String, String> jobToClassName;
  private final Map<String, URL[]> jobToClassLoaderURLs;

  private Props commonJobProps;
  private Props commonLoadProps;

  /**
   * Base constructor
   */
  public JobTypePluginSet() {
    this.pluginJobPropsMap = new HashMap<>();
    this.pluginLoadPropsMap = new HashMap<>();
    this.pluginPrivatePropsMap = new HashMap<>();
    this.pluginJobPropsProcessor = new HashMap<>();
    this.jobToClassName = new HashMap<>();
    this.jobToClassLoaderURLs = new HashMap<>();
  }

  /**
   * Copy constructor
   */
  public JobTypePluginSet(final JobTypePluginSet clone) {
    this.pluginJobPropsMap = new HashMap<>(clone.pluginJobPropsMap);
    this.pluginLoadPropsMap = new HashMap<>(clone.pluginLoadPropsMap);
    this.pluginPrivatePropsMap = new HashMap<>(clone.pluginPrivatePropsMap);
    this.commonJobProps = clone.commonJobProps;
    this.commonLoadProps = clone.commonLoadProps;
    this.pluginJobPropsProcessor = clone.pluginJobPropsProcessor;
    this.jobToClassName = clone.jobToClassName;
    this.jobToClassLoaderURLs = clone.jobToClassLoaderURLs;
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

  public void addPluginClassName(final String jobTypeName, final String jobTypeClassName) {
    this.jobToClassName.put(jobTypeName, jobTypeClassName);
  }

  /**
   * Gets the plugin job class name
   */
  public String getPluginClassName(final String jobTypeName) {
    return this.jobToClassName.get(jobTypeName);
  }

  /**
   * Get the resource URLs that should be added to its associated job ClassLoader.
   */
  public URL[] getPluginClassLoaderURLs(final String jobTypeName) {
    return this.jobToClassLoaderURLs.getOrDefault(jobTypeName, EMPTY_URLS);
  }

  /**
   * Adds plugin job properties used as default runtime properties
   */
  public void addPluginJobProps(final String jobTypeName, final Props props) {
    this.pluginJobPropsMap.put(jobTypeName, props);
  }

  /**
   * Add resource URLs that should be made available to ClassLoader of all jobs of the given jobtype.
   */
  public void addPluginClassLoaderURLs(final String jobTypeName, final URL[] urls) {
    this.jobToClassLoaderURLs.put(jobTypeName, urls);
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
