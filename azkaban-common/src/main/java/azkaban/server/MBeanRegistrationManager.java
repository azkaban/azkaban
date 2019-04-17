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
package azkaban.server;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.log4j.Logger;


/**
 * A class to manager MBean Registration tasks
 */
public class MBeanRegistrationManager {
  private static final Logger logger = Logger.getLogger(MBeanRegistrationManager.class);

  private List<ObjectName> registeredMBeans = new ArrayList<>();
  private MBeanServer mbeanServer = null;

  public void registerMBean(final String name, final Object mbean) {
    final Class<?> mbeanClass = mbean.getClass();
    final ObjectName mbeanName;
    try {
      mbeanName = new ObjectName(mbeanClass.getName() + ":name=" + name);
      getMbeanServer().registerMBean(mbean, mbeanName);
      logger.info("Bean " + mbeanClass.getCanonicalName() + " registered.");
      this.registeredMBeans.add(mbeanName);
    } catch (final Exception e) {
      logger.error("Error registering mbean " + mbeanClass.getCanonicalName(), e);
    }
  }

  /**
   * Get Platform MBeanServer Instance
   * @return platform MBeanServer Instance
   */
  public MBeanServer getMbeanServer() {
    return (mbeanServer == null)
        ? ManagementFactory.getPlatformMBeanServer()
        : mbeanServer;
  }

  /**
   * Close all registered MBeans
   */
  public void closeMBeans() {
    try {
      for (final ObjectName name : registeredMBeans) {
        getMbeanServer().unregisterMBean(name);
        logger.info("Jmx MBean " + name.getCanonicalName() + " unregistered.");
      }
    } catch (final Exception e) {
      logger.error("Failed to cleanup MBeanServer", e);
    }
  }

  /**
   * Get MBean Names
   * @return list of MBean Names
   */
  public List<ObjectName> getMBeanNames() {
    return registeredMBeans;
  }

  /**
   * Get MBeanInfo
   * @param name mbean Name
   * @return MBeanInfo
   */
  public MBeanInfo getMBeanInfo(final ObjectName name) {
    try {
      return getMbeanServer().getMBeanInfo(name);
    } catch (final Exception e) {
      logger.error(e);
      return null;
    }
  }

  /**
   * Get MBean Attribute
   * @param name mbean name
   * @param attribute attribute name
   * @return object of MBean Attribute
   */
  public Object getMBeanAttribute(final ObjectName name, final String attribute) {
    try {
      return getMbeanServer().getAttribute(name, attribute);
    } catch (final Exception e) {
      logger.error(e);
      return null;
    }
  }

  /**
   * Get MBean Result
   * @param mbeanName mbeanName
   * @return Map of MBean
   */
  public Map<String, Object> getMBeanResult(final String mbeanName) {
    final Map<String, Object> ret = new HashMap<>();
    try {
      final ObjectName name = new ObjectName(mbeanName);
      final MBeanInfo info = getMBeanInfo(name);

      final MBeanAttributeInfo[] mbeanAttrs = info.getAttributes();
      final Map<String, Object> attributes = new TreeMap<>();

      for (final MBeanAttributeInfo attrInfo : mbeanAttrs) {
        final Object obj = getMBeanAttribute(name, attrInfo.getName());
        attributes.put(attrInfo.getName(), obj);
      }

      ret.put("attributes", attributes);
    } catch (final Exception e) {
      logger.error(e);
      ret.put("error", "'" + mbeanName + "' is not a valid mBean name");
    }

    return ret;
  }
}
