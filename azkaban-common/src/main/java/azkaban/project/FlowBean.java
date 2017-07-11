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

package azkaban.project;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This is the top level class which is used by the YAML loader to deserialize a flow.yml file.
 */
public class FlowBean implements Serializable {

  private Map<String, String> config;
  private List<NodeBean> nodes;

  public Map<String, String> getConfig() {
    return this.config;
  }

  public void setConfig(final Map<String, String> config) {
    this.config = config;
  }

  public List<NodeBean> getNodes() {
    return this.nodes;
  }

  public void setNodes(final List<NodeBean> nodes) {
    this.nodes = nodes;
  }

  @Override
  public String toString() {
    return "Flow{nodes=" + this.nodes + ", config=" + this.config + '}';
  }
}
