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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

/**
 * FlowTriggerDependency is the logic representation of a trigger dependency.
 * It couldn't be changed once gets constructed.
 * It will be used to create running dependency instance.
 */
public class FlowTriggerDependency implements Serializable {

  private static final long serialVersionUID = 5875910030716100311L;
  private final Map<String, String> props;
  private final String name;
  private final String type;

  /**
   * @throws IllegalArgumentException if name or type is null or blank
   * @throws IllegalArgumentException if depProps is null
   */
  public FlowTriggerDependency(final String name, final String type, final Map<String, String>
      depProps) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name));
    Preconditions.checkArgument(StringUtils.isNotBlank(type));
    Preconditions.checkArgument(depProps != null);
    this.name = name;
    this.type = type;
    this.props = Collections.unmodifiableMap(depProps);
    //todo chengren311: validate per dependencyType: some dependency type might need extra special
    //check, also check if it's a valid dependency type
  }

  public String getName() {
    return this.name;
  }

  public String getType() {
    return this.type;
  }

  public Map<String, String> getProps() {
    return this.props;
  }

  @Override
  public String toString() {
    return "FlowTriggerDependency{" +
        "name='" + this.name + '\'' +
        ", type='" + this.type + '\'' +
        ", props=" + this.props +
        '}';
  }
}
