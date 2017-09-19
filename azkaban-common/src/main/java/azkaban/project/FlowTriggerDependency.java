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
 */

package azkaban.project;

import azkaban.utils.Props;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

/**
 * Dependency is an immutable class which holds
 * all the data and properties of a dependency.
 */
public class FlowTriggerDependency {

  private final Props props;
  private final String name;
  private final String type;

  public FlowTriggerDependency(final String name, final String type, final Props depProps) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(name));
    Preconditions.checkArgument(StringUtils.isNotEmpty(type));
    validateProps(depProps);
    this.name = name;
    this.type = type;
    this.props = new Props(depProps.getParent(), depProps);
    //todo chengren311: validate per dependencyType: daliviewdepenency needs extra special check:
    //e.x viewname format validation
    //e.x also check if it's a valid dependency type
  }

  private void validateProps(final Props props) {
    Preconditions.checkNotNull(props, "props shouldn't be null");
  }

  public String getName() {
    return this.name;
  }

  public String getType() {
    return this.type;
  }

  /**
   * @return an immutable copy of props
   */
  public Props getProps() {
    return new Props(this.props.getParent(), this.props);
  }

  @Override
  public String toString() {
    return "FlowTriggerDependency{" +
        "props=" + this.props +
        ", name='" + this.name + '\'' +
        ", type='" + this.type + '\'' +
        '}';
  }
}
