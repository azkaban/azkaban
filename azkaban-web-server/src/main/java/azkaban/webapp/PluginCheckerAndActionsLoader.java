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
 *
 */

package azkaban.webapp;

import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class PluginCheckerAndActionsLoader {

  private static final Logger log = Logger.getLogger(PluginCheckerAndActionsLoader.class);

  public void load(final String pluginPath) {
    log.info("Loading plug-in checker and action types");
    final File triggerPluginPath = new File(pluginPath);
    if (!triggerPluginPath.exists()) {
      log.error("plugin path " + pluginPath + " doesn't exist!");
      return;
    }

    final ClassLoader parentLoader = getClass().getClassLoader();
    final File[] pluginDirs = triggerPluginPath.listFiles();
    final ArrayList<String> jarPaths = new ArrayList<>();
    for (final File pluginDir : pluginDirs) {
      if (!pluginDir.exists()) {
        log.error("Error! Trigger plugin path " + pluginDir.getPath()
            + " doesn't exist.");
        continue;
      }

      if (!pluginDir.isDirectory()) {
        log.error("The plugin path " + pluginDir + " is not a directory.");
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
          log.error("Plugin conf file " + propertiesFile + " not found.");
          continue;
        }
      } else {
        log.error("Plugin conf path " + propertiesDir + " not found.");
        continue;
      }

      final List<String> extLibClasspath =
          pluginProps.getStringList("trigger.external.classpaths",
              (List<String>) null);

      final String pluginClass = pluginProps.getString("trigger.class");
      if (pluginClass == null) {
        log.error("Trigger class is not set.");
      } else {
        log.error("Plugin class " + pluginClass);
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
            log.error(e);
          }
        }
        if (extLibClasspath != null) {
          for (final String extLib : extLibClasspath) {
            try {
              final File file = new File(pluginDir, extLib);
              final URL url = file.toURI().toURL();
              urls.add(url);
            } catch (final MalformedURLException e) {
              log.error(e);
            }
          }
        }

        urlClassLoader =
            new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
      } else {
        log.error("Library path " + propertiesDir + " not found.");
        continue;
      }

      Class<?> triggerClass = null;
      try {
        triggerClass = urlClassLoader.loadClass(pluginClass);
      } catch (final ClassNotFoundException e) {
        log.error("Class " + pluginClass + " not found.");
        continue;
      }

      final String source = FileIOUtils.getSourcePathFromClass(triggerClass);
      log.info("Source jar " + source);
      jarPaths.add("jar:file:" + source);

      try {
        Utils.invokeStaticMethod(urlClassLoader, pluginClass,
            "initiateCheckerTypes", pluginProps, this);
      } catch (final Exception e) {
        log.error("Unable to initiate checker types for " + pluginClass);
        continue;
      }

      try {
        Utils.invokeStaticMethod(urlClassLoader, pluginClass,
            "initiateActionTypes", pluginProps, this);
      } catch (final Exception e) {
        log.error("Unable to initiate action types for " + pluginClass);
        continue;
      }

    }
  }

}
