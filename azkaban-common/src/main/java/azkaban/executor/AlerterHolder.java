/*
 * Copyright 2017 LinkedIn Corp.
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
package azkaban.executor;

import azkaban.alert.Alerter;
import azkaban.spi.AzkabanException;
import azkaban.utils.Emailer;
import azkaban.utils.FileIOUtils;
import azkaban.utils.PluginUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class AlerterHolder {

  private static final Logger logger = LoggerFactory.getLogger(AlerterHolder.class);
  private final Map<String, Alerter> alerters;

  @Inject
  public AlerterHolder(final Props props, final Emailer mailAlerter) {
    final Map<String, Alerter> allAlerters = new HashMap<>();
    // Load built-in alerters.
    allAlerters.put("email", mailAlerter);
    // Load additional alerter plugins if any.
    final String customAlertersBaseDir = props.getString("alerter.plugin.dir", "plugins/alerter");
    allAlerters.putAll(loadCustomAlerters(customAlertersBaseDir));
    this.alerters = allAlerters;
    logger.info("Alerter plugins loaded: {}", this.alerters.keySet());
  }

  private Map<String, Alerter> loadCustomAlerters(final String alerterPluginsRootPath) {
    final File pluginsRootPath = new File(alerterPluginsRootPath);
    if (!pluginsRootPath.exists()) {
      return Collections.<String, Alerter>emptyMap();
    }

    final Map<String, Alerter> installedAlerterPlugins = new HashMap<>();
    final ClassLoader parentLoader = getClass().getClassLoader();
    final File[] pluginDirs = pluginsRootPath.listFiles();

    for (final File pluginDir : pluginDirs) {
      try {
        loadCustomAlerter(installedAlerterPlugins, parentLoader, pluginDir);
      } catch (final Exception e) {
        logger.error(String.format("Failed to load alerter plugin in '%s'.", pluginDir), e);
      }
    }
    return installedAlerterPlugins;
  }

  private void loadCustomAlerter(final Map<String, Alerter> installedAlerterPlugins,
      final ClassLoader parentLoader, final File pluginDir) {
    // Load plugin properties.
    final Props pluginProps = PropsUtils.loadPluginProps(pluginDir);
    if (pluginProps == null) {
      throw new AzkabanException("Plugin config properties could not be loaded.");
    }

    final String pluginName = pluginProps.getString("alerter.name", "").trim();
    if (pluginName.isEmpty()) {
      throw new AzkabanException("Alerter name is required.");
    }

    final List<String> extLibClassPaths = pluginProps.getStringList(
        "alerter.external.classpaths", (List<String>) null);

    final String pluginClass = pluginProps.getString("alerter.class", "").trim();
    if (pluginClass.isEmpty()) {
      throw new AzkabanException("Alerter class is required.");
    }

    final Class<?> alerterClass =
        PluginUtils.getPluginClass(pluginClass, pluginDir, extLibClassPaths, parentLoader);
    if (alerterClass == null) {
      throw new AzkabanException(
          String.format("Alerter class '%s' could not be loaded.", pluginClass));
    }
    logger.info("Loaded alerter class '{}' from '{}'.", pluginClass,
        FileIOUtils.getSourcePathFromClass(alerterClass));

    Constructor<?> constructor = null;
    try {
      constructor = alerterClass.getConstructor(Props.class);
    } catch (final NoSuchMethodException e) {
      throw new AzkabanException("Alerter class constructor wasn't found.", e);
    }

    final Object obj;
    try {
      obj = constructor.newInstance(pluginProps);
    } catch (final Exception e) {
      throw new AzkabanException(String.format("Alerter class '%s' could not be instantiated.",
          pluginClass), e);
    }

    if (!(obj instanceof Alerter)) {
      throw new AzkabanException("Instantiated object is not an Alerter.");
    }
    installedAlerterPlugins.put(pluginName, (Alerter) obj);
  }

  public Alerter get(final String alerterType) {
    return this.alerters.get(alerterType);
  }

  public void forEach(final BiConsumer<String, Alerter> consumer) {
    this.alerters.forEach(consumer);
  }
}
