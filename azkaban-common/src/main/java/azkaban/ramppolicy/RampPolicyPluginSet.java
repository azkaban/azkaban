/*
 * Copyright 2019 LinkedIn Corp.
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
package azkaban.ramppolicy;

import azkaban.utils.Props;
import java.util.HashMap;
import java.util.Map;


/**
 * Container for ramp policy plugins
 *
 * This contains the RampPolicy Class objects, the properties for loading plugins, and the properties given
 * by default to the plugin.
 *
 * This class is not thread safe, so adding to this class should only be populated and controlled by
 * the RampPolicyManager
 */
public class RampPolicyPluginSet {
  private final Map<String, Class<? extends RampPolicy>> rampPolicyToClass;
  private final Map<String, Props> pluginCommonPropsMap;
  private final Map<String, Props> pluginLoadPropsMap;

  private Props commonProps;
  private Props commonLoadProps;

  /**
   * Base constructor
   */
  public RampPolicyPluginSet() {
    this.rampPolicyToClass = new HashMap<>();
    this.pluginCommonPropsMap = new HashMap<>();
    this.pluginLoadPropsMap = new HashMap<>();
  }

  /**
   * Copy constructor
   */
  public RampPolicyPluginSet(final RampPolicyPluginSet clone) {
    this.rampPolicyToClass = new HashMap<>(clone.rampPolicyToClass);
    this.pluginCommonPropsMap = new HashMap<>(clone.pluginCommonPropsMap);
    this.pluginLoadPropsMap = new HashMap<>(clone.pluginLoadPropsMap);
    this.commonProps = clone.commonProps;
    this.commonLoadProps = clone.commonLoadProps;
  }

  /**
   * Gets common properties for every ramp policy
   */
  public Props getCommonPluginProps() {
    return this.commonProps;
  }

  /**
   * Sets the common properties shared in every ramp policy
   */
  public void setCommonPluginProps(final Props commonProps) {
    this.commonProps = commonProps;
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
   * Get the properties for a ramp policy used to setup and load a plugin
   */
  public Props getPluginLoadProps(final String rampPolicyName) {
    return this.pluginLoadPropsMap.get(rampPolicyName);
  }

  /**
   * Get the properties that will be given to the plugin as common properties.
   */
  public Props getPluginCommonProps(final String rampPolicyName) {
    return this.pluginCommonPropsMap.get(rampPolicyName);
  }

  /**
   * Gets the plugin ramp policy class
   */
  public Class<? extends RampPolicy> getPluginClass(final String rampPolicyName) {
    return this.rampPolicyToClass.get(rampPolicyName);
  }

  /**
   * Adds plugin ramp-policy class
   */
  public void addPluginClass(final String rampPolicyName,
      final Class<? extends RampPolicy> rampPolicyClass) {
    this.rampPolicyToClass.put(rampPolicyName, rampPolicyClass);
  }

  /**
   * Adds plugin common properties used as default runtime properties. if props is null, nothing will be added.
   *
   * @param rampPolicyName ramp policy name
   * @param props nullable props
   */
  public void addPluginProps(final String rampPolicyName, final Props props) {
    if (props != null) {
      this.pluginCommonPropsMap.put(rampPolicyName, props);
    }
  }

  /**
   * Adds plugin load properties used to load the plugin. if props is null, nothing will be added.
   *
   * @param rampPolicyName ramp policy name
   * @param props nullable props
   */
  public void addPluginLoadProps(final String rampPolicyName, final Props props) {
    if (props != null) {
      this.pluginLoadPropsMap.put(rampPolicyName, props);
    }
  }
}
