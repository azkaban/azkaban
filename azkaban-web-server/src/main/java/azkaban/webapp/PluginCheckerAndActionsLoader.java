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
import azkaban.utils.PluginUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import java.io.File;
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
      // load plugin properties
      final Props pluginProps = PropsUtils.loadPluginProps(pluginDir, "Trigger");
      if (pluginProps == null) {
        continue;
      }

      final List<String> extLibClasspath =
          pluginProps.getStringList("trigger.external.classpaths",
              (List<String>) null);

      final String pluginClass = pluginProps.getString("trigger.class");
      if (pluginClass == null) {
        log.error("Trigger class is not set.");
        continue;
      } else {
        log.info("Plugin class " + pluginClass);
      }

      URLClassLoader urlClassLoader = PluginUtils.getURLClassLoader(pluginDir, extLibClasspath, parentLoader);
      if (urlClassLoader == null) {
        continue;
      }

      Class<?> triggerClass = PluginUtils.getPluginClass(urlClassLoader, pluginClass);
      if (triggerClass == null) {
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
