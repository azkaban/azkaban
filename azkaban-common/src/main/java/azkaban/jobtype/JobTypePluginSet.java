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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

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
  // Mapping for jobType to defaultProxyUser
  private final Map<String, String> jobToDefaultProxyUser;
  private final Map<String, URL[]> jobToClassLoaderURLs;

  // List of jobType classes which are allowed for the defaultProxyUsers feature
  private final Set<String> defaultProxyUsersJobTypeClasses;
  // List of users which are not allowed as defaultProxyUsers
  private final Set<String> defaultProxyUsersFilter;

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
    this.jobToDefaultProxyUser = new HashMap<>();
    this.defaultProxyUsersJobTypeClasses = new HashSet<>();
    this.defaultProxyUsersFilter = new HashSet<>();
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
    this.jobToDefaultProxyUser = clone.jobToDefaultProxyUser;
    this.defaultProxyUsersJobTypeClasses = clone.defaultProxyUsersJobTypeClasses;
    this.defaultProxyUsersFilter = clone.defaultProxyUsersFilter;
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

  /**
   * @return The Set of users which are not allowed as defaultProxyUsers.
   */
  public Set<String> getDefaultProxyUsersFilter() {
    return this.defaultProxyUsersFilter;
  }

  /**
   * @param user to be added to the list of users which are not allowed as defaultProxyUsers.
   */
  public void addDefaultProxyUsersFilter(String user) {
    this.defaultProxyUsersFilter.add(user);
  }

  /**
   * @param users to be added to the list of users which are not allowed as defaultProxyUsers.
   */
  public void addDefaultProxyUsersFilter(List<String> users) {
    this.defaultProxyUsersFilter.addAll(users);
  }

  /**
   * @param jobTypeClass The jobTypeClass to be added to the list of allowed jobType * classes for
   *                     the defaultProxyUsers feature.
   */
  public void addDefaultProxyUsersJobTypeClasses(String jobTypeClass) {
    this.defaultProxyUsersJobTypeClasses.add(jobTypeClass);
  }

  /**
   * @param jobTypeClasses The list of jobTypeClasses to be added to the list of allowed jobType
   *                       classes for the defaultProxyUsers feature.
   */
  public void addDefaultProxyUsersJobTypeClasses(List<String> jobTypeClasses) {
    this.defaultProxyUsersJobTypeClasses.addAll(jobTypeClasses);
  }

  /**
   * @return The list of allowed jobType classes for the defaultProxyUsers feature.
   */
  public Set<String> getDefaultProxyUsersJobTypeClasses() {
    return this.defaultProxyUsersJobTypeClasses;
  }

  /**
   * If the default proxy user is configured for the jobType and the jobType class associated is
   * part of allowed jobType classes for the defaultProxyUser feature, then return Optional
   * defaultProxyUser. Otherwise return Optional.empty().
   *
   * @param jobType The type of the job like hadoopJava, hadoopShell, hive, java, etc.
   * @return {@link Optional} defaultProxyUser corresponding to the jobType.
   */
  public Optional<String> getDefaultProxyUser(String jobType) {
    String defaultProxyUser = this.jobToDefaultProxyUser.get(jobType);

    if (StringUtils.isBlank(defaultProxyUser)) {
      return Optional.empty();
    }

    if (this.defaultProxyUsersFilter.contains(defaultProxyUser)) {
      return Optional.empty();
    }

    if (!this.jobToClassName.containsKey(jobType)) {
      return Optional.empty();
    }

    String jobTypeClassName = this.jobToClassName.get(jobType);
    if (!this.defaultProxyUsersJobTypeClasses.contains(jobTypeClassName)) {
      return Optional.empty();
    }
    return Optional.of(defaultProxyUser);
  }

  /**
   * Adds the defaultProxyUser for the jobType to the jobToDefaultProxyUser Map.
   *
   * @param jobType          The type of the job like hadoopJava, hadoopShell, hive, java, etc.
   * @param defaultProxyUser defaultProxyUser corresponding to the jobType.
   */
  public void addDefaultProxyUser(String jobType, String defaultProxyUser) {
    this.jobToDefaultProxyUser.put(jobType, defaultProxyUser);
  }
}
