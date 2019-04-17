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
import azkaban.utils.Emailer;
import azkaban.utils.FileIOUtils;
import azkaban.utils.PluginUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;


@Singleton
public class AlerterHolder {

  private static final Logger logger = Logger.getLogger(AlerterHolder.class);
  private Map<String, Alerter> alerters;

  @Inject
  public AlerterHolder(final Props props, final Emailer mailAlerter) {
    try {
      this.alerters = loadAlerters(props, mailAlerter);
    } catch (final Exception ex) {
      logger.error(ex);
      this.alerters = new HashMap<>();
    }
  }

  private Map<String, Alerter> loadAlerters(final Props props, final Emailer mailAlerter) {
    final Map<String, Alerter> allAlerters = new HashMap<>();
    // load built-in alerters
    allAlerters.put("email", mailAlerter);
    // load all plugin alerters
    final String pluginDir = props.getString("alerter.plugin.dir", "plugins/alerter");
    allAlerters.putAll(loadPluginAlerters(pluginDir));
    return allAlerters;
  }

  private Map<String, Alerter> loadPluginAlerters(final String pluginPath) {
    final File alerterPluginPath = new File(pluginPath);
    if (!alerterPluginPath.exists()) {
      return Collections.<String, Alerter>emptyMap();
    }

    final Map<String, Alerter> installedAlerterPlugins = new HashMap<>();
    final ClassLoader parentLoader = getClass().getClassLoader();
    final File[] pluginDirs = alerterPluginPath.listFiles();
    final ArrayList<String> jarPaths = new ArrayList<>();

    for (final File pluginDir : pluginDirs) {
      // load plugin properties
      final Props pluginProps = PropsUtils.loadPluginProps(pluginDir);
      if (pluginProps == null) {
        continue;
      }

      final String pluginName = pluginProps.getString("alerter.name");
      final List<String> extLibClasspath =
          pluginProps.getStringList("alerter.external.classpaths",
              (List<String>) null);

      final String pluginClass = pluginProps.getString("alerter.class");
      if (pluginClass == null) {
        logger.error("Alerter class is not set.");
        continue;
      } else {
        logger.info("Plugin class " + pluginClass);
      }

      URLClassLoader urlClassLoader = PluginUtils.getURLClassLoader(pluginDir, extLibClasspath, parentLoader);
      if (urlClassLoader == null) {
        continue;
      }

      Class<?> alerterClass = PluginUtils.getPluginClass(urlClassLoader, pluginClass);
      if (alerterClass == null) {
        continue;
      }

      final String source = FileIOUtils.getSourcePathFromClass(alerterClass);
      logger.info("Source jar " + source);
      jarPaths.add("jar:file:" + source);

      Constructor<?> constructor = null;
      try {
        constructor = alerterClass.getConstructor(Props.class);
      } catch (final NoSuchMethodException e) {
        logger.error("Constructor not found in " + pluginClass);
        continue;
      }

      Object obj = null;
      try {
        obj = constructor.newInstance(pluginProps);
      } catch (final Exception e) {
        logger.error(e);
      }

      if (!(obj instanceof Alerter)) {
        logger.error("The object is not an Alerter");
        continue;
      }

      final Alerter plugin = (Alerter) obj;
      installedAlerterPlugins.put(pluginName, plugin);
    }

    return installedAlerterPlugins;
  }

  public Alerter get(final String alerterType) {
    return this.alerters.get(alerterType);
  }
}
