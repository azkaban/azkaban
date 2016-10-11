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

package azkaban.webapp.plugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class PluginRegistry {

  private static PluginRegistry registry;

  public TreeSet<ViewerPlugin> viewerPlugins;

  public Map<String, TreeSet<ViewerPlugin>> jobTypeViewerPlugins;

  private PluginRegistry() {
    viewerPlugins = new TreeSet<ViewerPlugin>(ViewerPlugin.COMPARATOR);
    jobTypeViewerPlugins = new HashMap<String, TreeSet<ViewerPlugin>>();
  }

  public void register(ViewerPlugin plugin) {
    viewerPlugins.add(plugin);
    List<String> jobTypes = plugin.getJobTypes();
    if (jobTypes == null) {
      return;
    }

    for (String jobType : jobTypes) {
      TreeSet<ViewerPlugin> plugins = null;
      if (!jobTypeViewerPlugins.containsKey(jobType)) {
        plugins = new TreeSet<ViewerPlugin>(ViewerPlugin.COMPARATOR);
        plugins.add(plugin);
        jobTypeViewerPlugins.put(jobType, plugins);
      } else {
        plugins = jobTypeViewerPlugins.get(jobType);
        plugins.add(plugin);
      }
    }
  }

  public List<ViewerPlugin> getViewerPlugins() {
    return new ArrayList<ViewerPlugin>(viewerPlugins);
  }

  public List<ViewerPlugin> getViewerPluginsForJobType(String jobType) {
    TreeSet<ViewerPlugin> plugins = jobTypeViewerPlugins.get(jobType);
    if (plugins == null) {
      return null;
    }
    return new ArrayList<ViewerPlugin>(plugins);
  }

  public static PluginRegistry getRegistry() {
    if (registry == null) {
      registry = new PluginRegistry();
    }
    return registry;
  }
}
