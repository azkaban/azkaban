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

import azkaban.Constants;
import azkaban.utils.DIUtils;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Ramp Policy Manager will manager the Ramp strategy which will be applied upon Global Configuration Management
 */
public class RampPolicyManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RampPolicyManager.class);

  private final String pluginsDir; // the dir for ramp policy plugins
  private final ClassLoader parentClassLoader;
  private final Props globalProperties;
  private RampPolicyPluginSet pluginSet;

  public RampPolicyManager(final String pluginsDir, final Props globalProperties,
      final ClassLoader parentClassLoader) {
    this.pluginsDir = pluginsDir;
    this.globalProperties = globalProperties;
    this.parentClassLoader = parentClassLoader;
    loadPlugins();
  }

  public void loadPlugins() throws RampPolicyManagerException {
    final RampPolicyPluginSet plugins = new RampPolicyPluginSet();

    loadDefaultTypes(plugins);
    if (this.pluginsDir != null) {
      final File pluginsDir = new File(this.pluginsDir);

      if (FileIOUtils.isValidDirectory(pluginsDir)) {
        try {
          LOGGER.info("Ramp policy plugins directory set. Loading extra ramp policies from " + pluginsDir);
          loadPluginRampPolicies(plugins, pluginsDir);
        } catch (final Exception e) {
          LOGGER.info("Ramp Policy Plugins failed to load. " + e.getCause(), e);
          throw new RampPolicyManagerException(e);
        }
      }
    }

    // Swap the plugin set. If exception is thrown, then plugin isn't swapped.
    synchronized (this) {
      this.pluginSet = plugins;
    }
  }

  // load default RampPolicy, which are the NOOP ramp policy and the FULL ramp policy
  private void loadDefaultTypes(final RampPolicyPluginSet plugins)
      throws RampPolicyManagerException {
    LOGGER.info("Loading plugin default ramp policies");
    plugins.addPluginClass(NoopRampPolicy.class.getSimpleName(), NoopRampPolicy.class);
    plugins.addPluginClass(FullRampPolicy.class.getSimpleName(), FullRampPolicy.class);
    plugins.addPluginClass(SimpleAutoRampPolicy.class.getSimpleName(), SimpleAutoRampPolicy.class);
    plugins.addPluginClass(SimpleQuickRampPolicy.class.getSimpleName(), SimpleQuickRampPolicy.class);
  }

  // load ramp policies from ramp policy plugin dir
  private void loadPluginRampPolicies(final RampPolicyPluginSet plugins, final File pluginsDir)
      throws RampPolicyManagerException {

    // Load the common properties used by all ramp policies that are run
    try {
      plugins.setCommonPluginProps(
          new Props(this.globalProperties, new File(pluginsDir, Constants.PluginManager.COMMONCONFFILE))
      );
    } catch (IOException e) {
      throw new RampPolicyManagerException("Failed to load common plugin job properties" + e.getCause());
    }

    // Loads the common properties used by all ramp policy plugins when loading
    try {
      plugins.setCommonPluginLoadProps(
          new Props(null, new File(pluginsDir, Constants.PluginManager.COMMONSYSCONFFILE))
      );
    } catch (IOException e) {
      throw new RampPolicyManagerException("Failed to load common plugin loader properties" + e.getCause());
    }

    // Loading ramp policy plugins
    for (final File pluginDir : pluginsDir.listFiles()) {
      if (FileIOUtils.isValidDirectory(pluginDir)) {
        try {
          loadRampPolicies(pluginDir, plugins);
        } catch (final Exception e) {
          LOGGER.error("Failed to load ramp policy " + pluginDir.getName() + e.getMessage(), e);
          throw new RampPolicyManagerException(e);
        }
      }
    }
  }

  private void loadRampPolicies(final File pluginDir, final RampPolicyPluginSet plugins)
      throws RampPolicyManagerException {
    // Directory is the ramp policy name
    final String rampPolicyName = pluginDir.getName();
    LOGGER.info("Loading plugin " + rampPolicyName);

    final File pluginPropsFile = new File(pluginDir, Constants.PluginManager.CONFFILE);
    final File pluginLoadPropsFile = new File(pluginDir, Constants.PluginManager.SYSCONFFILE);

    if (!pluginPropsFile.exists()) {
      LOGGER.info("Plugin load props file " + pluginPropsFile + " not found.");
      return;
    }

    Props pluginProps = null;
    Props pluginLoadProps = null;
    try {
      pluginProps = new Props(plugins.getCommonPluginProps(), pluginPropsFile);
      // Adding "plugin.dir" to allow plugin.properties file could read this property. Also, user
      // code could leverage this property as well.
      pluginProps.put("plugin.dir", pluginDir.getAbsolutePath());
      plugins.addPluginProps(rampPolicyName, pluginProps);

      pluginLoadProps = new Props(plugins.getCommonPluginLoadProps(),  pluginLoadPropsFile);
      pluginLoadProps.put("plugin.dir", pluginDir.getAbsolutePath());
      pluginLoadProps = PropsUtils.resolveProps(pluginLoadProps);
      // Add properties into the plugin set
      plugins.addPluginLoadProps(rampPolicyName, pluginLoadProps);
    } catch (final Exception e) {
      LOGGER.error("pluginLoadProps to help with debugging: " + pluginLoadProps);
      throw new RampPolicyManagerException("Failed to get ramp policy properties" + e.getMessage(), e);
    }

    final ClassLoader rampPolicyClassLoader =
        loadRampPolicyClassLoader(pluginDir, rampPolicyName, plugins);
    final String rampPolicyClass = pluginLoadProps.get("ramppolicy.class");

    Class<? extends RampPolicy> clazz = null;
    try {
      clazz = (Class<? extends RampPolicy>) rampPolicyClassLoader.loadClass(rampPolicyClass);
      plugins.addPluginClass(rampPolicyName, clazz);
    } catch (final ClassNotFoundException e) {
      throw new RampPolicyManagerException(e);
    }

    LOGGER.info("Verifying ramp policy plugin " + rampPolicyName);

    try {
      final Props fakeSysProps = new Props(pluginLoadProps);
      final Props fakeProps = new Props(pluginProps);
      final RampPolicy obj = (RampPolicy) Utils.callConstructor(clazz, fakeSysProps, fakeProps);
    } catch (final Throwable t) {
      LOGGER.info("RampPolicy " + rampPolicyName + " failed test!", t);
      throw new RampPolicyExecutionException(t);
    }

    LOGGER.info("Loaded ramp policy " + rampPolicyName + " " + rampPolicyClass);
  }

  /**
   * Creates and loads all plugin resources (jars) into a ClassLoader
   */
  private ClassLoader loadRampPolicyClassLoader(final File pluginDir,
      final String rampPolicyName, final RampPolicyPluginSet plugins) {
    // sysconf says what jars/confs to load
    final Props pluginLoadProps = plugins.getPluginLoadProps(rampPolicyName);
    final List<URL> resources = new ArrayList<>();

    try {
      final List<String> typeGlobalClassPath =
          pluginLoadProps.getStringList("ramppolicy.global.classpath", null, ",");

      final List<String> typeClassPath =
          pluginLoadProps.getStringList("ramppolicy.classpath", null, ",");

      final List<String> ramppolicyLibDirs =
          pluginLoadProps.getStringList("ramppolicy.lib.dir", null, ",");

      resources.addAll(DIUtils.loadResources(
          pluginDir, typeGlobalClassPath, typeClassPath, ramppolicyLibDirs
      ));

    } catch (final MalformedURLException e) {
      throw new RampPolicyManagerException(e);
    }

    // each ramp policy can have a different class loader
    LOGGER.info(
        String.format("Classpath for plugin[dir: %s, ramp-policy: %s]: %s", pluginDir, rampPolicyName, resources)
    );

    return DIUtils.getClassLoader(resources, this.parentClassLoader);
  }


  public RampPolicy buildRampPolicyExecutor(final String rampPolicyName,  Props props) {

    final Class<? extends Object> executorClass = pluginSet.getPluginClass(rampPolicyName);
    if (executorClass == null) {
      throw new RampPolicyExecutionException(
          String.format("Ramp Policy is unrecognized. Could not construct ramp policy [%s]", rampPolicyName)
      );
    }

    Props commonProps = pluginSet.getPluginCommonProps(rampPolicyName);
    if (commonProps == null) {
      commonProps = pluginSet.getCommonPluginProps();
    }
    if (commonProps != null) {
      for (final String k : commonProps.getKeySet()) {
        if (!props.containsKey(k)) {
          props.put(k, commonProps.get(k));
        }
      }
    }
    props = PropsUtils.resolveProps(props);

    Props loadProps = pluginSet.getPluginCommonProps(rampPolicyName);
    if (loadProps != null) {
      loadProps = PropsUtils.resolveProps(loadProps);
    } else {
      loadProps = pluginSet.getCommonPluginLoadProps();
      if (loadProps == null) {
        loadProps = new Props();
      }
    }

    RampPolicy rampPolicy = (RampPolicy) Utils.callConstructor(executorClass, loadProps, props);

    return rampPolicy;
  }

  /**
   * Public for test reasons. Will need to move tests to the same package
   */
  public synchronized RampPolicyPluginSet getRampPolicyPluginSet() {
    return this.pluginSet;
  }
}
