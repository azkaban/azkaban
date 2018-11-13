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

import static java.util.Objects.requireNonNull;

import azkaban.utils.Props;
import java.util.List;

/**
 * A unit of execution that could be either a job or a flow.
 */
public abstract class AzkabanNode {

  protected final String name;
  protected final String type;
  protected final Props props;
  protected final String condition;
  protected final List<String> dependsOn;

  public AzkabanNode(final String name, final String type, final Props props, final String
      condition, final List<String>
      dependsOn) {
    this.name = requireNonNull(name);
    this.type = requireNonNull(type);
    this.props = requireNonNull(props);
    this.condition = condition;
    this.dependsOn = dependsOn;
  }

  public String getName() {
    return this.name;
  }

  public String getType() {
    return this.type;
  }

  public Props getProps() {
    return this.props;
  }

  public String getCondition() {
    return this.condition;
  }

  public List<String> getDependsOn() {
    return this.dependsOn;
  }
}
