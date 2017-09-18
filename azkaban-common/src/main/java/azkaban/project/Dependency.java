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

import azkaban.Constants;
import azkaban.utils.Props;
import com.google.common.base.Preconditions;

/**
 * Dependency is an immutable class which holds
 * all the data and properties of a dependency.
 */
public class Dependency {

  private final Props props;

  public Dependency(final Props depProps) {
    validateProps(depProps);
    this.props = new Props(depProps.getParent(), depProps);
    //todo chengren311: validate per dependencyType: daliviewdepenency needs extra special check:
    //e.x viewname format validation
    //e.x also check if it's a valid dependency type
  }

  private void validateProps(final Props props) {
    Preconditions.checkNotNull(props);
    Preconditions.checkNotNull(props.get(Constants.DependencyProperties.DEPENDENCY_NAME));
    Preconditions.checkNotNull(props.get(Constants.DependencyProperties.DEPENDENCY_TYPE));
  }

  public String getName() {
    return this.props.get(Constants.DependencyProperties.DEPENDENCY_NAME);
  }

  public String getType() {
    return this.props.get(Constants.DependencyProperties.DEPENDENCY_TYPE);
  }

  /**
   * @return an immutable copy of props
   */
  public Props getProps() {
    return new Props(this.props.getParent(), this.props);
  }

  @Override
  public String toString() {
    return "Dependency{" +
        "props=" + this.props +
        '}';
  }
}
