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

import azkaban.trigger.TriggerAgent;
import azkaban.webapp.servlet.AbstractAzkabanServlet;

public interface TriggerPlugin {

  // public TriggerPlugin(String pluginName, Props props, AzkabanWebServer
  // azkabanWebApp) {
  // this.pluginName = pluginName;
  // this.pluginPath = props.getString("trigger.path");
  // this.order = props.getInt("trigger.order", 0);
  // this.hidden = props.getBoolean("trigger.hidden", false);
  //
  // }

  public AbstractAzkabanServlet getServlet();

  public TriggerAgent getAgent();

  public void load();

  public String getPluginName();

  public String getPluginPath();

  public int getOrder();

  public boolean isHidden();

  public void setHidden(boolean hidden);

  public String getInputPanelVM();
}
