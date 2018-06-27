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

import azkaban.utils.Props;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;

/**
 * The smallest individual unit of execution in Azkaban.
 * Contains information about job type and related properties.
 */
public class AzkabanJob extends AzkabanNode {

  private AzkabanJob(final String name, final String type, final Props props,
      final String condition, final List<String> dependsOn) {
    super(name, type, props, condition, dependsOn);
  }

  public static class AzkabanJobBuilder {

    private String name;
    private String type;
    private Props props;
    private String condition;
    private List<String> dependsOn;

    public AzkabanJobBuilder name(final String name) {
      this.name = name;
      return this;
    }

    public AzkabanJobBuilder type(final String type) {
      this.type = type;
      return this;
    }

    public AzkabanJobBuilder props(final Props props) {
      this.props = props;
      return this;
    }

    public AzkabanJobBuilder condition(final String condition) {
      this.condition = condition;
      return this;
    }

    public AzkabanJobBuilder dependsOn(final List<String> dependsOn) {
      // A node may or may not have dependencies.
      this.dependsOn = dependsOn == null
          ? Collections.emptyList()
          : ImmutableList.copyOf(dependsOn);
      return this;
    }

    public AzkabanJob build() {
      return new AzkabanJob(this.name, this.type, this.props, this.condition, this.dependsOn);
    }
  }
}
