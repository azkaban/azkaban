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

import java.util.List;
import java.util.Map;

/**
 * The node bean is used by the YAML loader to deserialize DAG nodes
 */
public class NodeBean {

  private String name;
  private Map<String, String> config;
  private List<String> dependsOn;
  private String type;

  public String getName() {
    return this.name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public Map<String, String> getConfig() {
    return this.config;
  }

  public void setConfig(final Map<String, String> config) {
    this.config = config;
  }

  public List<String> getDependsOn() {
    return this.dependsOn;
  }

  public void setDependsOn(final List<String> dependsOn) {
    this.dependsOn = dependsOn;
  }

  public String getType() {
    return this.type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return "Node{config=" + this.config + ", dependsOn=" + this.dependsOn + '}';
  }
}
