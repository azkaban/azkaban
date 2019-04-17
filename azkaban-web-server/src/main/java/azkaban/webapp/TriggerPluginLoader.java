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

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import azkaban.utils.FileIOUtils;
import azkaban.utils.PluginUtils;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.webapp.plugin.TriggerPlugin;
import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.mortbay.jetty.servlet.Context;


public class TriggerPluginLoader {

  private static final Logger log = Logger.getLogger(TriggerPluginLoader.class);
  private final String pluginPath;

  public TriggerPluginLoader(final Props props) {
    this.pluginPath = props.getString("trigger.plugin.dir", "plugins/triggers");
  }

  public Map<String, TriggerPlugin> loadTriggerPlugins(final Context root) {
    /*
     * TODO spyne: TriggerPluginLoader should not have any dependency on Azkaban Web Server
     **/
    final AzkabanWebServer azkabanWebServer = SERVICE_PROVIDER.getInstance(AzkabanWebServer.class);
    final File triggerPluginPath = new File(this.pluginPath);
    if (!triggerPluginPath.exists()) {
      return new HashMap<>();
    }

    final Map<String, TriggerPlugin> installedTriggerPlugins = new HashMap<>();
    final ClassLoader parentLoader = AzkabanWebServer.class.getClassLoader();
    final File[] pluginDirs = triggerPluginPath.listFiles();
    final ArrayList<String> jarPaths = new ArrayList<>();

    for (final File pluginDir : pluginDirs) {
      // load plugin properties
      final Props pluginProps = PropsUtils.loadPluginProps(pluginDir);
      if (pluginProps == null) {
        continue;
      }

      final String pluginName = pluginProps.getString("trigger.name");
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

      Constructor<?> constructor = null;
      try {
        constructor = triggerClass.getConstructor(String.class, Props.class, Context.class, AzkabanWebServer.class);
      } catch (final NoSuchMethodException e) {
        log.error("Constructor not found in " + pluginClass);
        continue;
      }

      Object obj = null;
      try {
        obj = constructor.newInstance(pluginName, pluginProps, root, azkabanWebServer);
      } catch (final Exception e) {
        log.error(e);
      }

      if (!(obj instanceof TriggerPlugin)) {
        log.error("The object is not an TriggerPlugin");
        continue;
      }

      final TriggerPlugin plugin = (TriggerPlugin) obj;
      installedTriggerPlugins.put(pluginName, plugin);
    }

    // Velocity needs the jar resource paths to be set.
    final String jarResourcePath = StringUtils.join(jarPaths, ", ");
    log.info("Setting jar resource path " + jarResourcePath);
    final VelocityEngine ve = azkabanWebServer.getVelocityEngine();
    ve.addProperty("jar.resource.loader.path", jarResourcePath);

    return installedTriggerPlugins;
  }
}
