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
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
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

    final Map<String, Alerter> installedAlerterPlugins =
        new HashMap<>();
    final ClassLoader parentLoader = getClass().getClassLoader();
    final File[] pluginDirs = alerterPluginPath.listFiles();
    final ArrayList<String> jarPaths = new ArrayList<>();
    for (final File pluginDir : pluginDirs) {
      if (!pluginDir.isDirectory()) {
        logger.error("The plugin path " + pluginDir + " is not a directory.");
        continue;
      }

      // Load the conf directory
      final File propertiesDir = new File(pluginDir, "conf");
      Props pluginProps = null;
      if (propertiesDir.exists() && propertiesDir.isDirectory()) {
        final File propertiesFile = new File(propertiesDir, "plugin.properties");
        final File propertiesOverrideFile =
            new File(propertiesDir, "override.properties");

        if (propertiesFile.exists()) {
          if (propertiesOverrideFile.exists()) {
            pluginProps =
                PropsUtils.loadProps(null, propertiesFile,
                    propertiesOverrideFile);
          } else {
            pluginProps = PropsUtils.loadProps(null, propertiesFile);
          }
        } else {
          logger.error("Plugin conf file " + propertiesFile + " not found.");
          continue;
        }
      } else {
        logger.error("Plugin conf path " + propertiesDir + " not found.");
        continue;
      }

      final String pluginName = pluginProps.getString("alerter.name");
      final List<String> extLibClasspath =
          pluginProps.getStringList("alerter.external.classpaths",
              (List<String>) null);

      final String pluginClass = pluginProps.getString("alerter.class");
      if (pluginClass == null) {
        logger.error("Alerter class is not set.");
      } else {
        logger.info("Plugin class " + pluginClass);
      }

      URLClassLoader urlClassLoader = null;
      final File libDir = new File(pluginDir, "lib");
      if (libDir.exists() && libDir.isDirectory()) {
        final File[] files = libDir.listFiles();

        final ArrayList<URL> urls = new ArrayList<>();
        for (int i = 0; i < files.length; ++i) {
          try {
            final URL url = files[i].toURI().toURL();
            urls.add(url);
          } catch (final MalformedURLException e) {
            logger.error(e);
          }
        }
        if (extLibClasspath != null) {
          for (final String extLib : extLibClasspath) {
            try {
              final File file = new File(pluginDir, extLib);
              final URL url = file.toURI().toURL();
              urls.add(url);
            } catch (final MalformedURLException e) {
              logger.error(e);
            }
          }
        }

        urlClassLoader =
            new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
      } else {
        logger.error("Library path " + propertiesDir + " not found.");
        continue;
      }

      Class<?> alerterClass = null;
      try {
        alerterClass = urlClassLoader.loadClass(pluginClass);
      } catch (final ClassNotFoundException e) {
        logger.error("Class " + pluginClass + " not found.");
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
