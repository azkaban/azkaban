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

public abstract class AzkabanNode {

  protected final String name;
  protected final Props props;

  public AzkabanNode(final String name, final Props props) {
    this.name = requireNonNull(name);
    this.props = requireNonNull(props);
  }

  public String getName() {
    return name;
  }

  public Props getProps() {
    return props;
  }
}
