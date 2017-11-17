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

import java.util.Collections;
import java.util.Map;

/**
 * Java bean loaded from YAML file to represent a trigger dependency.
 */
public class TriggerDependencyBean {

  private String name;
  private String type;
  private Map<String, String> params;

  public String getName() {
    return this.name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getType() {
    return this.type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public Map<String, String> getParams() {
    return this.params == null ? Collections.emptyMap() : this.params;
  }

  public void setParams(final Map<String, String> params) {
    this.params = params;
  }

  @Override
  public String toString() {
    return "TriggerDependencyBean{" +
        "name='" + this.name + '\'' +
        ", type='" + this.type + '\'' +
        ", params=" + this.params +
        '}';
  }
}
