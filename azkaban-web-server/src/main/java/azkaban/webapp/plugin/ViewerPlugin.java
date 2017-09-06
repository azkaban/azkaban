/*
 * Copyright 2012 LinkedIn Corp.
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
import java.util.Comparator;
import java.util.List;

public class ViewerPlugin {

  public static final Comparator<ViewerPlugin> COMPARATOR =
      new Comparator<ViewerPlugin>() {
        @Override
        public int compare(final ViewerPlugin o1, final ViewerPlugin o2) {
          if (o1.getOrder() != o2.getOrder()) {
            return o1.getOrder() - o2.getOrder();
          }
          return o1.getPluginName().compareTo(o2.getPluginName());
        }
      };
  private final String pluginName;
  private final String pluginPath;
  private final int order;
  private final List<String> jobTypes;
  private boolean hidden;

  public ViewerPlugin(final String pluginName, final String pluginPath, final int order,
      final boolean hidden, final String jobTypes) {
    this.pluginName = pluginName;
    this.pluginPath = pluginPath;
    this.order = order;
    this.setHidden(hidden);
    this.jobTypes = parseJobTypes(jobTypes);
  }

  public String getPluginName() {
    return this.pluginName;
  }

  public String getPluginPath() {
    return this.pluginPath;
  }

  public int getOrder() {
    return this.order;
  }

  public boolean isHidden() {
    return this.hidden;
  }

  public void setHidden(final boolean hidden) {
    this.hidden = hidden;
  }

  protected List<String> parseJobTypes(final String jobTypesStr) {
    if (jobTypesStr == null) {
      return null;
    }
    final String[] parts = jobTypesStr.split(",");
    final List<String> jobTypes = new ArrayList<>();
    for (int i = 0; i < parts.length; ++i) {
      jobTypes.add(parts[i].trim());
    }
    return jobTypes;
  }

  public List<String> getJobTypes() {
    return this.jobTypes;
  }
}
