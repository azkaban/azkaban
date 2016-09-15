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
  private final String pluginName;
  private final String pluginPath;
  private final int order;
  private boolean hidden;
  private final List<String> jobTypes;

  public static final Comparator<ViewerPlugin> COMPARATOR =
      new Comparator<ViewerPlugin>() {
        @Override
        public int compare(ViewerPlugin o1, ViewerPlugin o2) {
          if (o1.getOrder() != o2.getOrder()) {
            return o1.getOrder() - o2.getOrder();
          }
          return o1.getPluginName().compareTo(o2.getPluginName());
        }
      };

  public ViewerPlugin(String pluginName, String pluginPath, int order,
      boolean hidden, String jobTypes) {
    this.pluginName = pluginName;
    this.pluginPath = pluginPath;
    this.order = order;
    this.setHidden(hidden);
    this.jobTypes = parseJobTypes(jobTypes);
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getPluginPath() {
    return pluginPath;
  }

  public int getOrder() {
    return order;
  }

  public boolean isHidden() {
    return hidden;
  }

  public void setHidden(boolean hidden) {
    this.hidden = hidden;
  }

  protected List<String> parseJobTypes(String jobTypesStr) {
    if (jobTypesStr == null) {
      return null;
    }
    String[] parts = jobTypesStr.split(",");
    List<String> jobTypes = new ArrayList<String>();
    for (int i = 0; i < parts.length; ++i) {
      jobTypes.add(parts[i].trim());
    }
    return jobTypes;
  }

  public List<String> getJobTypes() {
    return jobTypes;
  }
}
