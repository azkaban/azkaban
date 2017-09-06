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
    this.viewerPlugins = new TreeSet<>(ViewerPlugin.COMPARATOR);
    this.jobTypeViewerPlugins = new HashMap<>();
  }

  public static PluginRegistry getRegistry() {
    if (registry == null) {
      registry = new PluginRegistry();
    }
    return registry;
  }

  public void register(final ViewerPlugin plugin) {
    this.viewerPlugins.add(plugin);
    final List<String> jobTypes = plugin.getJobTypes();
    if (jobTypes == null) {
      return;
    }

    for (final String jobType : jobTypes) {
      TreeSet<ViewerPlugin> plugins = null;
      if (!this.jobTypeViewerPlugins.containsKey(jobType)) {
        plugins = new TreeSet<>(ViewerPlugin.COMPARATOR);
        plugins.add(plugin);
        this.jobTypeViewerPlugins.put(jobType, plugins);
      } else {
        plugins = this.jobTypeViewerPlugins.get(jobType);
        plugins.add(plugin);
      }
    }
  }

  public List<ViewerPlugin> getViewerPlugins() {
    return new ArrayList<>(this.viewerPlugins);
  }

  public List<ViewerPlugin> getViewerPluginsForJobType(final String jobType) {
    final TreeSet<ViewerPlugin> plugins = this.jobTypeViewerPlugins.get(jobType);
    if (plugins == null) {
      return null;
    }
    return new ArrayList<>(plugins);
  }
}
